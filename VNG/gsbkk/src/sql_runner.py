# -*- coding: utf-8 -*-
"""
SQL Runner - Simplified SQL-First ETL Processor

Replaces etl_engine.py for single-SQL transformation steps.
Supports all input patterns found in the L2M re-standardization pipeline.

Designed for Python 2.7 compatibility:
  - key=value positional argument parsing (no argparse)
  - .format() string interpolation (no f-strings)
  - No type hints, no pathlib

Usage (via spark-submit):
    spark-submit src/sql_runner.py key=value [key=value ...]

=== INPUT PATTERNS ===

Pattern A - Single HDFS file (default):
    input_path=hdfs://.../{logDate}   input_view=source_data
    sql_file=transform/re_standardization/l2m/cons/active.sql.j2

Pattern B - Multi HDFS files (STD active, server_performance):
    input_paths=login|hdfs://.../{logDate};logout|hdfs://.../{logDate};...
    sql_file=transform/re_standardization/l2m/std/active_std.sql.j2

Pattern C - JDBC extraction (ETL Trino tasks, snapshot tables):
    extract_sql_file=transform/re_standardization/l2m/etl/active_login.sql.j2
    extract_connection=GDS_TRINO
    extract_view=source_data
    (no sql_file needed - result written directly to output)

Pattern D - Single file + supplementary JDBC source (economy CONS tasks):
    input_path=hdfs://.../etl/item_gain/{logDate}   input_view=item_data
    secondary_sql_file=transform/game_health_check/gnoth/user_profile.sql.j2
    secondary_connection=GDS_POSTGRES
    secondary_view=user_profile
    sql_file=transform/re_standardization/l2m/cons/item_gain.sql.j2

Pattern E - Multi files + supplementary JDBC (server_performance):
    input_paths=active_tbl|hdfs://.../std/active/{logDate};revenue_tbl|hdfs://.../etl/recharge/{logDate}
    secondary_sql_file=transform/re_standardization/l2m/cons/role_profile_for_nru.sql.j2
    secondary_connection=GDS_POSTGRES
    secondary_view=role_profile
    sql_file=transform/re_standardization/l2m/cons/server_performance.sql.j2

=== OUTPUT PATTERNS ===

HDFS Parquet:
    output_type=file
    output_path=hdfs://.../{logDate}
    output_mode=overwrite|append

JDBC Postgres (idempotent):
    output_type=jdbc
    output_table=public.l2m_active
    output_connection=TSN_POSTGRES
    output_mode=append
    delete_condition="active_date = DATE '{{ logDate }}'"

=== ALL ARGS ===

    sql_file              SparkSQL Jinja2 template for transformation
    extract_sql_file      JDBC extraction query template (Trino / Postgres)
    extract_connection    JDBC connection for extraction  (GDS_TRINO, GDS_POSTGRES, TSN_POSTGRES)
    extract_view          Temp view name for extracted data  (default: source_data)
    input_path            Single HDFS input path (with {var} substitution)
    input_format          Format for input_path  (default: parquet)
    input_view            Temp view name for input_path  (default: source_data)
    input_paths           Multi-file: "view1|path1;view2|path2;..."
    secondary_sql_file    Supplementary JDBC source query template
    secondary_connection  JDBC connection for secondary source
    secondary_view        Temp view name for secondary source  (default: secondary)
    output_type           jdbc | file
    output_table          Target Postgres table  (e.g. public.l2m_active)
    output_path           Target HDFS path  (for output_type=file)
    output_connection     TSN_POSTGRES | GDS_POSTGRES  (default: TSN_POSTGRES)
    output_format         File format  (default: parquet)
    output_mode           overwrite | append  (default: overwrite)
    delete_condition      SQL WHERE for idempotent delete+insert
    num_partitions        Repartition count before write  (default: 0 = auto)
    gameId                Game identifier (JDBC database selection)
    logDate               Processing date (YYYY-MM-DD or YYYY-MM)

All key=value args are available as Jinja2 template variables.
"""

import json
import os
import sys
from functools import reduce
from pyspark.sql import SparkSession


HDFS_BASE = 'hdfs://c0s/user/gsbkk-workspace-yc9t6'


def handler(args):
    job = convert_args(args)

    sql_file            = job.get('sql_file', '')
    extract_sql_file    = job.get('extract_sql_file', '')
    extract_connection  = job.get('extract_connection', '')
    extract_view        = job.get('extract_view', 'source_data')
    input_path          = substitute_path(job.get('input_path', ''), job)
    input_format        = job.get('input_format', 'parquet')
    input_view          = job.get('input_view', 'source_data')
    input_paths         = job.get('input_paths', '')
    secondary_sql_file  = job.get('secondary_sql_file', '')
    secondary_connection = job.get('secondary_connection', '')
    secondary_view      = job.get('secondary_view', 'secondary')
    output_type         = job.get('output_type', 'file')
    output_table        = job.get('output_table', '')
    output_path         = substitute_path(job.get('output_path', ''), job)
    output_connection   = job.get('output_connection', 'TSN_POSTGRES')
    output_format       = job.get('output_format', 'parquet')
    output_mode         = job.get('output_mode', 'overwrite')
    delete_condition    = job.get('delete_condition', '')
    num_partitions      = int(job.get('num_partitions', '0'))

    spark = SparkSession.builder.getOrCreate()

    project_root = os.getenv('PROJECT_ROOT', '/opt/airflow/dags/repo')

    # -------------------------------------------------------------------------
    # Pattern C: JDBC extraction (Trino or Postgres → temp view)
    # -------------------------------------------------------------------------
    if extract_sql_file:
        print('JDBC extract: rendering {0}'.format(extract_sql_file))
        jdbc_sql = render_sql_file(extract_sql_file, job, project_root)
        print('JDBC query (first 500 chars):\n{0}'.format(jdbc_sql[:500]))
        jdbc_url, properties = get_jdbc_url(extract_connection, job)
        table_expr = '({0}) AS {1}'.format(jdbc_sql, extract_view)
        df_extract = spark.read.jdbc(url=jdbc_url, table=table_expr, properties=properties)
        df_extract.createOrReplaceTempView(extract_view)
        print('Registered JDBC temp view: {0} ({1} rows)'.format(extract_view, df_extract.count()))

    # -------------------------------------------------------------------------
    # Pattern B: multi-file inputs (view1|path1;view2|path2;...)
    # -------------------------------------------------------------------------
    if input_paths:
        for entry in input_paths.split(';'):
            entry = entry.strip()
            if not entry:
                continue
            pipe_idx = entry.index('|')
            view_name = entry[:pipe_idx]
            file_path = substitute_path(entry[pipe_idx + 1:], job)
            df_in = spark.read.format(input_format).load(file_path)
            df_in.createOrReplaceTempView(view_name)
            print('Registered temp view: {0} from {1} ({2} rows)'.format(
                view_name, file_path, df_in.count()))

    # -------------------------------------------------------------------------
    # Pattern A: single file input
    # -------------------------------------------------------------------------
    elif input_path:
        df_in = spark.read.format(input_format).load(input_path)
        df_in.createOrReplaceTempView(input_view)
        print('Registered temp view: {0} from {1} ({2} rows)'.format(
            input_view, input_path, df_in.count()))

    # -------------------------------------------------------------------------
    # Pattern D/E: supplementary JDBC source (e.g. user_profile, role_profile)
    # -------------------------------------------------------------------------
    if secondary_sql_file:
        print('Secondary JDBC: rendering {0}'.format(secondary_sql_file))
        sec_sql = render_sql_file(secondary_sql_file, job, project_root)
        sec_url, sec_props = get_jdbc_url(secondary_connection, job)
        sec_table = '({0}) AS {1}'.format(sec_sql, secondary_view)
        df_sec = spark.read.jdbc(url=sec_url, table=sec_table, properties=sec_props)
        df_sec.createOrReplaceTempView(secondary_view)
        print('Registered secondary temp view: {0} ({1} rows)'.format(
            secondary_view, df_sec.count()))

    # -------------------------------------------------------------------------
    # SparkSQL transformation (optional — if not set, write extract_view directly)
    # -------------------------------------------------------------------------
    if sql_file:
        print('SparkSQL: rendering {0}'.format(sql_file))
        spark_sql = render_sql_file(sql_file, job, project_root)
        print('Rendered SQL (first 500 chars):\n{0}'.format(spark_sql[:500]))
        df = spark.sql(spark_sql)
    elif extract_sql_file:
        # No SparkSQL — write extracted JDBC data directly to output
        df = spark.table(extract_view)
    elif input_path or input_paths:
        # No SQL — write input directly (pure format conversion)
        view_to_write = input_view if input_path else input_paths.split(';')[0].split('|')[0]
        df = spark.table(view_to_write)
    else:
        raise ValueError('No input source or sql_file provided')

    row_count = df.count()
    print('Result: {0} rows'.format(row_count))

    if num_partitions > 0:
        df = df.repartition(num_partitions)

    # -------------------------------------------------------------------------
    # Write output
    # -------------------------------------------------------------------------
    if output_type == 'file':
        target = output_path or output_table
        if not target:
            raise ValueError('output_path or output_table required for output_type=file')
        df.write.format(output_format).mode(output_mode).save(target)
        print('Written to {0}'.format(target))

    elif output_type == 'jdbc':
        if not output_table:
            raise ValueError('output_table required for output_type=jdbc')
        write_jdbc(spark, df, output_connection, output_table,
                   output_mode, delete_condition, job)
    else:
        raise ValueError('Unsupported output_type: {0}'.format(output_type))

    spark.stop()


# =============================================================================
# JDBC WRITE (idempotent delete+insert)
# =============================================================================

def write_jdbc(spark, df, connection, table, mode, delete_condition, job):
    """Write DataFrame to JDBC with optional idempotent delete+insert."""
    jdbc_url, properties = get_jdbc_url(connection, job)

    if delete_condition and mode == 'append':
        # Render Jinja2 in delete_condition (e.g. {{ logDate }})
        resolved_condition = parse_template_str(delete_condition, job)

        df.cache()
        row_count = df.count()

        if row_count == 0:
            print('WARNING: No rows to write. Skipping.')
            df.unpersist()
            return

        table_name = table.split('.')[-1]
        temp_table = 'temp_gsbkk_{0}'.format(table_name)

        print('Staging {0} rows to temp table: {1}'.format(row_count, temp_table))
        df.write.jdbc(url=jdbc_url, table=temp_table, mode='overwrite', properties=properties)

        conn = None
        stmt = None
        try:
            conn = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(
                jdbc_url, properties['user'], properties['password']
            )
            conn.setAutoCommit(False)
            stmt = conn.createStatement()

            parts = table.split('.') if '.' in table else [None, table]
            schema_name = parts[0]
            tbl_name    = parts[-1]
            meta = conn.getMetaData()
            rs   = meta.getTables(None, schema_name, tbl_name, None)
            table_exists = rs.next()
            rs.close()

            if table_exists:
                delete_sql = 'DELETE FROM {0} WHERE {1}'.format(table, resolved_condition)
                insert_sql = 'INSERT INTO {0} SELECT * FROM {1}'.format(table, temp_table)
                print('DELETE: {0}'.format(delete_sql))
                stmt.executeUpdate(delete_sql)
                print('INSERT: {0}'.format(insert_sql))
                stmt.executeUpdate(insert_sql)
            else:
                create_sql = 'CREATE TABLE {0} AS SELECT * FROM {1}'.format(table, temp_table)
                print('CREATE TABLE: {0}'.format(create_sql))
                stmt.executeUpdate(create_sql)

            conn.commit()
            print('Transaction committed: {0} rows to {1}'.format(row_count, table))

        except Exception as ex:
            print('ERROR: Transaction failed: {0}'.format(ex))
            if conn:
                conn.rollback()
                print('Transaction rolled back')
            raise
        finally:
            if stmt:
                stmt.close()
            if conn:
                conn.close()
            df.unpersist()

    else:
        df.write.jdbc(url=jdbc_url, table=table, mode=mode, properties=properties)
        print('Written {0} rows to {1}'.format(df.count(), table))


# =============================================================================
# CREDENTIALS
# =============================================================================

def get_jdbc_url(connection, job):
    """Load credentials and return (jdbc_url, properties_dict)."""
    creds_dir = os.getenv('CREDENTIALS_DIR', '/tmp/gsbkk_creds')

    if connection == 'GDS_TRINO':
        cred_file = os.path.join(creds_dir, 'cred_trino.json')
        with open(cred_file, 'r') as fh:
            creds = json.loads(fh.read())
        trino = creds.get('trino', creds)
        url   = 'jdbc:trino://{0}:{1}/iceberg/default?SSL=true&SSLVerification=NONE'.format(
            trino['host'], trino['port'])
        props = {'user': trino['user'], 'password': creds.get('password', ''),
                 'driver': 'io.trino.jdbc.TrinoDriver'}
        return url, props

    elif connection == 'TSN_POSTGRES':
        cred_file = os.path.join(creds_dir, 'cred_tsn.json')
        with open(cred_file, 'r') as fh:
            creds = json.loads(fh.read())
        pg       = creds.get('postgres', creds)
        database = job.get('gameId', 'sensortower')
        url      = 'jdbc:postgresql://{0}:{1}/{2}'.format(pg['host'], pg['port'], database)
        props    = {'user': pg['user'], 'password': pg['password'],
                    'driver': 'org.postgresql.Driver'}
        return url, props

    elif connection == 'GDS_POSTGRES':
        cred_file = os.path.join(creds_dir, 'cred_gds.json')
        with open(cred_file, 'r') as fh:
            creds = json.loads(fh.read())
        pg       = creds.get('postgres', creds)
        database = job.get('gameId', 'sensortower')
        url      = 'jdbc:postgresql://{0}:{1}/{2}'.format(pg['host'], pg['port'], database)
        props    = {'user': pg['user'], 'password': pg['password'],
                    'driver': 'org.postgresql.Driver'}
        return url, props

    else:
        raise ValueError('Unknown connection: {0}'.format(connection))


# =============================================================================
# TEMPLATE RENDERING
# =============================================================================

def render_sql_file(sql_file, vars_dict, project_root):
    """
    Render a Jinja2 SQL template from the local filesystem.
    Uses FileSystemLoader so {% import 'sql/macros.sql' %} works.
    """
    from jinja2 import Environment, FileSystemLoader

    if sql_file.startswith('hdfs://'):
        # HDFS file: read via Spark then fall back to simple string rendering
        # (imports not supported for HDFS templates)
        spark = SparkSession.builder.getOrCreate()
        content = ''.join(spark.sparkContext.textFile(sql_file).collect())
        return parse_template_str(content, vars_dict)

    sql_path = os.path.join(project_root, sql_file)
    template_dir  = os.path.dirname(sql_path)
    template_name = os.path.basename(sql_path)

    search_paths = [
        template_dir,
        os.path.join(project_root, 'transform', 'common'),  # macros.sql
        os.path.join(project_root, 'transform'),
    ]
    env      = Environment(loader=FileSystemLoader(search_paths))
    template = env.get_template(template_name)

    # Provide both camelCase and snake_case aliases for compatibility
    render_vars = dict(vars_dict)
    if 'logDate' in render_vars:
        render_vars['log_date'] = render_vars['logDate']
    if 'gameId' in render_vars:
        render_vars['game_id'] = render_vars['gameId']

    return template.render(**render_vars)


def parse_template_str(template_str, vars_dict):
    """Render a Jinja2 template from a string (no filesystem imports)."""
    from jinja2 import Environment
    env = Environment()
    return env.from_string(template_str).render(**vars_dict)


# =============================================================================
# UTILS
# =============================================================================

def substitute_path(path, job):
    """Replace {key} placeholders in an HDFS path using job args dict."""
    result = path
    for key, value in job.items():
        result = result.replace('{' + key + '}', str(value))
    return result


def convert_args(args):
    """Convert ['key=value', ...] list into a dict."""
    def combine(d, arg):
        parts = arg.split('=', 1)
        d[parts[0]] = parts[1]
        return d
    return reduce(combine, args, {})


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise ValueError('No arguments passed. Run with: sql_runner.py key=value ...')
    handler(sys.argv[1:])
