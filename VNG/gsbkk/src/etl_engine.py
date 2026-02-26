#!/usr/bin/env python3
"""
ETL Engine - Layout-based Transformation Processor

Processes JSON layout files with PySpark for standardized data transformations.

Architecture: Layout JSON → Variable Substitution → Read Data → Transform → Write
Supports: Multiple I/O (file/JDBC), SQL templates, partitioning

Usage: python3 etl_engine.py --layout layouts/path.json --vars "gameId=l2m,logDate=2024-12"
"""

from pyspark.sql import SparkSession
import argparse
import json
import os
import sys
import logging

# Add parent directory to path for imports when running via spark-submit
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import from modular core components
from src.core.templates import render_sql_template, resolve_table_path, substitute_variables
from src.core.loaders import get_jdbc_url

# Set up logging
logger = logging.getLogger(__name__)


def expand_date_range(path, variables):
    """
    Expand date range patterns in paths.
    
    Supports patterns like:
    - {logDate(-29,0)} → {2026-01-01,2026-01-02,...,2026-01-30} (30 days)
    - {logDate(-1,0)} → {2026-01-29,2026-01-30} (2 days)
    
    Args:
        path: Path string potentially containing date range patterns
        variables: Dict containing logDate and other variables
    
    Returns:
        Path with date ranges expanded
    """
    import re
    from datetime import datetime, timedelta
    
    # Pattern: {varName(start,end)} where start and end are day offsets
    pattern = r'\{(\w+)\((-?\d+),(-?\d+)\)\}'
    
    def replace_range(match):
        var_name = match.group(1)
        start_offset = int(match.group(2))
        end_offset = int(match.group(3))
        
        # Get base date from variables
        base_date_str = variables.get(var_name)
        if not base_date_str:
            return match.group(0)  # Return original if variable not found
        
        try:
            base_date = datetime.strptime(base_date_str, '%Y-%m-%d')
        except ValueError:
            return match.group(0)  # Return original if date format invalid
        
        # Generate date range
        dates = []
        for offset in range(start_offset, end_offset + 1):
            date = base_date + timedelta(days=offset)
            dates.append(date.strftime('%Y-%m-%d'))
        
        # Return as brace expansion: {date1,date2,date3}
        return '{' + ','.join(dates) + '}'
    
    return re.sub(pattern, replace_range, path)


# =============================================================================
# LEGACY WRAPPER FUNCTIONS
# These functions delegate to src.core modules but maintain backward compatibility
# =============================================================================

# resolve_table_path is now imported from src.core.templates

# render_sql_template is now imported from src.core.templates

# get_jdbc_url is now imported from src.core.loaders


def load_layout(layout_path: str, variables: dict) -> dict:
    """
    Load layout JSON and substitute variables ({gameId}, {logDate}, etc.)

    Args:
        layout_path: Path to layout JSON file
        variables: Variables to substitute

    Returns:
        Parsed JSON with variables substituted
    """
    with open(layout_path, 'r') as f:
        layout_str = f.read()

    # Load as JSON
    config = json.loads(layout_str)

    # Render SQL template if present
    if 'sqlTemplate' in config:
        sql_template_path = config['sqlTemplate']
        sql_query = render_sql_template(sql_template_path, variables)
        variables['sqlQuery'] = sql_query
        print(f"Rendered SQL template: {sql_template_path}")

    # Substitute variables recursively using core utility
    return substitute_variables(config, variables)


def process_layout(spark: SparkSession, layout_path: str, variables: dict):
    """
    Process a layout file with PySpark
    Supports both single input (inputType/inputPath) and multiple inputs (inputSources)

    Args:
        spark: SparkSession
        layout_path: Path to layout JSON file
        variables: Dictionary of variables to substitute
    """
    print(f"Processing layout: {layout_path}")
    print(f"Variables: {variables}")

    # Load layout configuration
    config = load_layout(layout_path, variables)

    # Note: Spark configuration is now applied during session creation in main()
    # to avoid "Cannot modify config" errors for immutable configs

    # Get inputSources array
    input_sources = config.get('inputSources', [])

    if input_sources:
        # MULTI-SOURCE MODE: Read multiple sources and register as temp views
        print(f"Multi-source mode: {len(input_sources)} sources")

        for source in input_sources:
            source_name = source.get('name')
            source_type = source.get('type', 'file')

            print(f"Loading source: {source_name} (type: {source_type})")

            if source_type == 'file':
                path = source.get('path', '')
                
                # First expand date range patterns like {logDate(-29,0)}
                path = expand_date_range(path, variables)
                
                # Then substitute simple variables
                for key, value in variables.items():
                    path = path.replace(f"{{{key}}}", str(value))

                format = source.get('format', 'parquet')
                df = spark.read.format(format).load(path)

            elif source_type == 'jdbc':
                connection = source.get('connection', 'TSN_POSTGRES')
                jdbc_url, properties = get_jdbc_url(connection, variables)

                # Override with custom properties if provided
                if 'properties' in source:
                    properties.update(source['properties'])

                # Check for SQL template or direct query
                sql_template = source.get('sqlTemplate', '')
                query = source.get('query', '')

                if sql_template:
                    # Prepare template variables with table path if tableKey is present
                    table_key = source.get('tableKey', '')
                    template_vars = variables.copy()

                    if table_key:
                        game_id = variables.get('gameId', '')
                        table_path = resolve_table_path(game_id, table_key)
                        template_vars['table_path'] = table_path
                        print(
                            f"Resolved table: {game_id}.{table_key} → {table_path}")

                    # Render SQL template (moved out of if table_key block)
                    query = render_sql_template(sql_template, template_vars)
                    print(f"Rendered SQL template: {sql_template}")
                    print(f"Full SQL query:\n{query}")

                # Substitute variables in query (only if query is not empty)
                if query:
                    for key, value in variables.items():
                        query = query.replace(f"{{{key}}}", str(value))

                if query:
                    table = f"({query}) AS {source_name}"
                    print(f"JDBC Query (first 200 chars): {table[:200]}...")
                    if len(query) > 200:
                        print(f"... (total {len(query)} characters)")
                else:
                    table = source.get('table', '')
                    if not table:
                        raise ValueError(
                            f"JDBC source '{source_name}' must have either 'query', 'sqlTemplate', or 'table' defined")
                    # Substitute variables in table field
                    for key, value in variables.items():
                        table = table.replace(f"{{{key}}}", str(value))

                print(f"JDBC: {jdbc_url} / table={table[:100]}...")
                df = spark.read.jdbc(
                    url=jdbc_url, table=table, properties=properties)
                
                # DEBUG: Show query plan to verify GROUP BY pushdown
                if query and 'GROUP BY' in query.upper():
                    print(
                        "DEBUG: Query contains GROUP BY - checking if pushed down to database...")
                    print("Physical plan:")
                    df.explain(mode="formatted")

            elif source_type == 'gsheet':
                # Read from Google Sheets
                sheet_id = source.get('sheet_id', '')
                if not sheet_id:
                    raise ValueError(f"GSheet source '{source_name}' must have 'sheet_id' defined")
                
                # Substitute variables in sheet_id (in case it's parameterized)
                for key, value in variables.items():
                    sheet_id = sheet_id.replace(f"{{{key}}}", str(value))
                
                # Get range (default to entire sheet)
                range_name = source.get('range', 'A:Z')
                
                # Parse range to get worksheet name (e.g., "Sheet1!A:Z" -> "Sheet1")
                if '!' in range_name:
                    worksheet_name = range_name.split('!')[0]
                else:
                    worksheet_name = 'Sheet1'  # Default worksheet name
                
                print(f"GSheet: Reading from {sheet_id}/{worksheet_name} (range: {range_name})")
                
                # Import and initialize GSheetWriter
                from src.core.writers import GSheetWriter
                
                # Get credentials path
                creds_dir = os.getenv('CREDENTIALS_DIR', '/tmp/gsbkk_creds')
                cred_file = f"{creds_dir}/gsheet_creds.json"
                
                if not os.path.exists(cred_file):
                    raise ValueError(f"Google Sheets credentials not found at {cred_file}")
                
                gsheet_reader = GSheetWriter(cred_file)
                df = gsheet_reader.read(sheet_id, worksheet_name)
                
                print(f"Read {df.count()} rows from Google Sheet")

            else:
                raise ValueError(f"Unsupported source type: {source_type}")

            # Cache DataFrame if specified (useful for JDBC sources used in complex queries)
            if source.get('cache', False):
                print(f"Caching DataFrame: {source_name}")
                df = df.cache()

            # Register as temp view
            df.createOrReplaceTempView(source_name)
            row_count = df.count()
            print(f"Registered temp view: {source_name} ({row_count} rows)")

        # Apply layout-level SQL template if specified
        layout_sql_template = config.get('sqlTemplate', '')
        if layout_sql_template:
            # Resolve SQL template path (relative to project root, not layout directory)
            if not os.path.isabs(layout_sql_template):
                project_root = os.getenv(
                    'PROJECT_ROOT', '/opt/airflow/dags/repo')
                layout_sql_template = os.path.join(project_root, layout_sql_template)

            print(f"Applying SQL transformation: {layout_sql_template}")
            # Render Jinja2 template with variables
            sql_query = render_sql_template(layout_sql_template, variables)

            df = spark.sql(sql_query)
            print(f"SQL result: {df.count()} rows")
        else:
            # No SQL template, use the first source as the output dataframe
            df = spark.table(input_sources[0]['name'])
    else:
        raise ValueError(
            "Layout must contain 'inputSources' array. Please update to modern format.")

    # Write outputs
    outputs_config = config.get('outputs', [])

    if not outputs_config:
        raise ValueError(
            "Layout must contain 'outputs' array. Please update to modern format.")

    # Process each output
    for idx, output_cfg in enumerate(outputs_config):
        output_name = output_cfg.get('name', f'output_{idx+1}')
        print(f"\n>>> Processing output: {output_name}")

        output_type = output_cfg.get('type', 'file')
        output_path = output_cfg.get('path', output_cfg.get('table', ''))
        output_format = output_cfg.get('format', 'parquet')
        mode = output_cfg.get('mode', 'overwrite')
        delete_condition = output_cfg.get('deleteCondition', '')

        num_partitions = output_cfg.get(
            'numPartitions', config.get('numPartitions', None))

        # Substitute variables in output path and delete condition
        for key, value in variables.items():
            output_path = output_path.replace(f"{{{key}}}", str(value))
            if delete_condition:
                delete_condition = delete_condition.replace(
                    f"{{{key}}}", str(value))

        print(
            f"Writing to: {output_path} (type: {output_type}, format: {output_format}, mode: {mode})")

        # Repartition if specified
        df_to_write = df
        if num_partitions:
            df_to_write = df.repartition(num_partitions)

        # Write output based on type
        write_output(spark, df_to_write, output_type, output_path, output_format, mode,
                     delete_condition, output_cfg, config, variables)


def write_output(spark, df, output_type, output_path, output_format, mode,
                 delete_condition, output_config, config, variables):
    """Write DataFrame to specified output destination"""

    # Add updated_at column if requested
    if output_config.get('add_updated_at', False):
        from pyspark.sql.functions import current_timestamp
        df = df.withColumn('updated_at', current_timestamp())
        print("Added 'updated_at' column with current timestamp")

    # Write output
    if output_type == 'file':
        # Get partition columns if specified
        partition_cols = output_config.get('partitionBy', [])
        
        if partition_cols:
            # Manual partitioning: write to subdirectories without column name prefix
            # e.g., /retention/2026-01-03/ instead of /retention/report_date=2026-01-03/
            print(f"Writing with manual partitioning by: {partition_cols}")
            
            # Get unique values for the first partition column
            partition_col = partition_cols[0]  # Support single partition column for now
            partition_values = df.select(partition_col).distinct().collect()
            
            for row in partition_values:
                partition_value = row[partition_col]
                partition_path = f"{output_path}/{partition_value}"
                
                # Filter data for this partition
                partition_df = df.filter(df[partition_col] == partition_value)
                
                # Write to partition-specific path (always overwrite each partition)
                partition_df.write.format(output_format).mode('overwrite').save(partition_path)
                print(f"Written {partition_df.count()} rows to {partition_path}")
        else:
            # No partitioning - write directly
            df.write.format(output_format).mode(mode).save(output_path)
            print(f"Successfully written {df.count()} rows to {output_path}")

    elif output_type == 'jdbc':
        connection = output_config.get(
            'connection', config.get('outputConnection', ''))
        jdbc_url, properties = get_jdbc_url(connection, variables)

        # Override with custom properties
        if 'properties' in output_config:
            properties.update(output_config['properties'])
        elif 'outputProperties' in config:
            properties.update(config['outputProperties'])

        # Handle atomic delete + insert for specific date
        if delete_condition and mode == 'append':
            print(
                f"Atomic delete+insert mode for condition: {delete_condition}")

            # Cache DataFrame
            df.cache()
            row_count = df.count()

            if row_count == 0:
                print("WARNING: No data to write. Skipping.")
                df.unpersist()
                return

            # Create temp table with fixed name (will be overwritten each run)
            # Format: temp_gsbkk_{target_table_name}
            table_name = output_path.split('.')[-1]
            temp_table = f"temp_gsbkk_{table_name}"

            print(f"Writing {row_count} rows to temp table: {temp_table}")
            df.write.jdbc(url=jdbc_url, table=temp_table,
                          mode='overwrite', properties=properties)

            # Execute transaction: DELETE specific date + INSERT
            print("Executing transaction...")
            conn = None
            stmt = None
            try:
                conn = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(
                    jdbc_url, properties['user'], properties['password']
                )
                conn.setAutoCommit(False)
                stmt = conn.createStatement()

                # Check if table exists using database metadata
                schema_name, table_name = output_path.split(
                    '.') if '.' in output_path else (None, output_path)
                metadata = conn.getMetaData()
                rs = metadata.getTables(None, schema_name, table_name, None)
                table_exists = rs.next()
                rs.close()
                print(f"Table {output_path} exists: {table_exists}")

                # Delete only records matching condition if table exists
                if table_exists:
                    delete_query = f"DELETE FROM {output_path} WHERE {delete_condition}"
                    print(f"DELETE: {delete_query}")
                    deleted_rows = stmt.executeUpdate(delete_query)
                    print(
                        f"Deleted {deleted_rows} rows for condition: {delete_condition}")

                # Insert new data or create table if it doesn't exist
                if table_exists:
                    insert_query = f"INSERT INTO {output_path} SELECT * FROM {temp_table}"
                    print(f"INSERT: {insert_query}")
                else:
                    insert_query = f"CREATE TABLE {output_path} AS SELECT * FROM {temp_table}"
                    print(f"CREATE TABLE: {insert_query}")

                stmt.executeUpdate(insert_query)

                # Commit
                conn.commit()
                print(
                    f"Transaction committed: {row_count} rows {'inserted' if table_exists else 'created'}")

            except Exception as e:
                print(f"ERROR: Transaction failed: {e}")
                if conn:
                    conn.rollback()
                    print("Transaction rolled back")
                raise
            finally:
                # No need to drop temp table - it will be overwritten next run
                # This saves storage space and avoids accumulation of temp tables
                if stmt:
                    stmt.close()
                if conn:
                    conn.close()
                df.unpersist()

        else:
            # Simple JDBC write
            print(f"Writing to JDBC: {jdbc_url} / {output_path}")
            df.write.jdbc(url=jdbc_url, table=output_path,
                          mode=mode, properties=properties)
            print(f"Successfully written {df.count()} rows")

    elif output_type == 'gsheet':
        # Write to Google Sheets using gsheet_processor
        print(f"Writing to Google Sheets...")

        # Import gsheet processor
        import os
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        from gsheet_processor import write_to_gsheet

        # Use output_config directly as gsheet config (all settings inline)
        gsheet_config = {
            'sheet_id': output_config.get('sheet_id', ''),
            'range': output_config.get('range', 'Sheet1!A1'),
            'columns': output_config.get('columns', df.columns),
            'sort_by': output_config.get('sort_by', []),
            'update_mode': output_config.get('update_mode', 'overwrite'),
            'key_column': output_config.get('key_column', 'report_date')
        }

        # Write to Google Sheets (pass DataFrame directly)
        write_to_gsheet(spark, gsheet_config, df_input=df)
        print(
            f"Successfully written to Google Sheets: {gsheet_config.get('sheet_id', '')}")

    else:
        raise ValueError(f"Unsupported output type: {output_type}")

    print("Layout processing completed!")


def main():
    """Main entry point for CLI"""
    parser = argparse.ArgumentParser(
        description='ETL Engine - Process layout files with variable substitution'
    )
    parser.add_argument(
        '--layout',
        required=True,
        help='Path to layout JSON file (e.g., layouts/sensortower/etl/top_games.json)'
    )
    parser.add_argument(
        '--vars',
        required=False,
        help='Variables in key=value,key2=value2 format (e.g., gameId=l2m,logDate=2025-12-25)'
    )

    args = parser.parse_args()

    # Parse variables
    variables = {}
    if args.vars:
        for var_pair in args.vars.split(','):
            if '=' in var_pair:
                key, value = var_pair.split('=', 1)
                variables[key.strip()] = value.strip()

    # NOTE: Table name mappings removed - no longer needed after rolling_forecast
    # and game_health_check refactoring. Templates now use hardcoded table names.
    # Only re-standardization still uses tableKey resolution via resolve_table_path()

    # Load layout to get Spark configuration before creating session
    try:
        with open(args.layout, 'r') as f:
            layout_config = json.load(f)
        spark_config = layout_config.get('sparkConfig', {})
    except Exception as e:
        print(f"Warning: Could not load layout for Spark config: {e}")
        spark_config = {}

    # Create Spark session with layout-specific configs
    builder = SparkSession.builder \
        .appName("GSBKK ETL Engine") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Apply layout-specific Spark configs at session creation time
    if spark_config:
        print(f"Applying Spark configuration: {spark_config}")
        for key, value in spark_config.items():
            builder = builder.config(key, str(value))

    spark = builder.getOrCreate()

    try:
        # Process the layout
        process_layout(spark, args.layout, variables)
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: Layout processing failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
