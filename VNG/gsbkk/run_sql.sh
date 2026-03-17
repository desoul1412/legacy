#!/usr/bin/env bash
################################################################################
# SQL Runner Shell Wrapper
################################################################################
#
# PURPOSE:
#   Execute a SQL (Jinja2) transformation on Spark via sql_runner.py.
#   Replaces run_etl_process.sh for SQL-centric pipelines (no JSON layout needed).
#
# USAGE:
#   ./run_sql.sh key=value [key=value ...]
#
# REQUIRED ARGS:
#   sql_file=<path>       SQL template path (relative to repo root or hdfs://)
#   output_type=<type>    jdbc | file
#   logDate=<date>        Processing date (YYYY-MM-DD or YYYY-MM)
#
# COMMON OPTIONAL ARGS:
#   output_table=<table>       Target Postgres table (e.g. public.rfc_daily)
#   output_path=<hdfs_path>    Target HDFS path (for output_type=file)
#   output_connection=<conn>   TSN_POSTGRES | GDS_POSTGRES  (default: TSN_POSTGRES)
#   output_mode=<mode>         overwrite | append            (default: overwrite)
#   delete_condition=<sql>     WHERE clause for idempotent delete+insert
#   input_path=<hdfs_path>     Optional HDFS input to register as temp view
#   input_view=<view_name>     Temp view name for input_path (default: source)
#   gameId=<id>                Game identifier
#   num_partitions=<n>         Repartition before write
#
# EXAMPLES:
#   # HDFS parquet → Postgres (idempotent)
#   ./run_sql.sh \
#       sql_file=transform/cons/rfc_daily.sql.j2 \
#       output_type=jdbc \
#       output_table=public.rfc_daily \
#       output_mode=append \
#       delete_condition="EXTRACT(YEAR FROM date) = 2026" \
#       gameId=rfc \
#       logDate=2026-01-15
#
#   # HDFS parquet → HDFS parquet (ETL layer)
#   ./run_sql.sh \
#       sql_file=transform/etl/sensortower_top_games.sql.j2 \
#       input_path=hdfs://c0s/raw/sensortower/top_games/logDate=2026-01-15 \
#       input_view=raw_top_games \
#       output_type=file \
#       output_path=hdfs://c0s/user/gsbkk-workspace-yc9t6/etl/sensortower/top_games/logDate=2026-01-15 \
#       logDate=2026-01-15
#
################################################################################

set -e

# ==============================================================================
# ENVIRONMENT
# ==============================================================================

AIRFLOW_REPO=${AIRFLOW_REPO:="/opt/airflow/dags/repo"}
export AIRFLOW_HOME="/opt/airflow"
export HADOOP_CLIENT_OPTS="-Xmx2147483648 -Djava.net.preferIPv4Stack=true"
export HADOOP_CONF_DIR="/etc/hadoop"
export SPARK_HOME="/opt/spark"
export SPARK_CONF_DIR="/etc/spark"
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/hadoop/bin:/opt/hive/bin:/opt/spark/bin"
export PYTHONPATH="$PYTHONPATH:$AIRFLOW_REPO"
export HTTP_PROXY="http://proxy.dp.vng.vn:3128"
export HTTPS_PROXY="http://proxy.dp.vng.vn:3128"

# ==============================================================================
# HDFS PATHS
# ==============================================================================

HDFS_BASE="hdfs://c0s/user/gsbkk-workspace-yc9t6"
HDFS_ARCHIVES="$HDFS_BASE/archives"
HDFS_CONFIGS="$HDFS_BASE/configs"

PYTHON_ENV_TARBALL="$HDFS_ARCHIVES/environment.tar.gz"
CODE_TARBALL="$HDFS_ARCHIVES/gsbkk-src.tar.gz"
POSTGRES_JAR="$HDFS_BASE/postgresql-42.7.4.jar"
TRINO_JAR="$HDFS_BASE/trino-jdbc-368.jar"

# ==============================================================================
# VALIDATE ARGS
# ==============================================================================

if [ $# -lt 1 ]; then
    echo "Usage: $0 key=value [key=value ...]"
    echo "  Required: sql_file=... output_type=... logDate=..."
    echo ""
    echo "Example:"
    echo "  $0 sql_file=transform/cons/rfc_daily.sql.j2 output_type=jdbc \\"
    echo "     output_table=public.rfc_daily output_mode=append \\"
    echo "     delete_condition=\"EXTRACT(YEAR FROM date) = 2026\" \\"
    echo "     gameId=rfc logDate=2026-01-15"
    exit 1
fi

# Extract sql_file and logDate for job naming
SQL_FILE=""
LOG_DATE=""
for arg in "$@"; do
    case "$arg" in
        sql_file=*) SQL_FILE="${arg#sql_file=}" ;;
        logDate=*)  LOG_DATE="${arg#logDate=}" ;;
    esac
done

echo "========================================================"
echo "SQL Runner"
echo "========================================================"
echo "SQL File : $SQL_FILE"
echo "Log Date : $LOG_DATE"
echo "All Args : $*"
echo "========================================================"

# ==============================================================================
# SETUP LOCAL ENVIRONMENT
# ==============================================================================

LOCAL_ENV_DIR="/tmp/gsbkk_env_$$"
TEMP_CREDS_DIR="/tmp/gsbkk_creds_$$"

mkdir -p "$LOCAL_ENV_DIR"
mkdir -p "$TEMP_CREDS_DIR"

echo ">>> Downloading credentials from HDFS..."
hdfs dfs -get "$HDFS_CONFIGS/cred_tsn.json" "$TEMP_CREDS_DIR/"  2>/dev/null || echo "TSN credentials not found"
hdfs dfs -get "$HDFS_CONFIGS/cred_gds.json" "$TEMP_CREDS_DIR/"  2>/dev/null || echo "GDS credentials not found"

export CREDENTIALS_DIR="$TEMP_CREDS_DIR"
export PROJECT_ROOT="$AIRFLOW_REPO"

echo ">>> Extracting Python environment..."
hdfs dfs -get "$PYTHON_ENV_TARBALL" "$LOCAL_ENV_DIR/environment.tar.gz"
tar -xzf "$LOCAL_ENV_DIR/environment.tar.gz" -C "$LOCAL_ENV_DIR"

export PYSPARK_DRIVER_PYTHON="$LOCAL_ENV_DIR/bin/python"
export PYSPARK_PYTHON="./environment/bin/python"

# ==============================================================================
# CODE TARBALL
# Always rebuild so Spark executors pick up the latest code from the repo.
# Without a CI/CD runner to pre-package code, a cached tarball would silently
# run stale code after every git push.
# ==============================================================================

echo ">>> Packaging repo code into tarball..."
LOCAL_TARBALL="/tmp/gsbkk-src-$$.tar.gz"
cd "$AIRFLOW_REPO"
tar -czf "$LOCAL_TARBALL" \
    --exclude='*.pyc' \
    --exclude='__pycache__' \
    --exclude='.git' \
    src/ transform/ configs/ templates/
hdfs dfs -put -f "$LOCAL_TARBALL" "$CODE_TARBALL"
rm -f "$LOCAL_TARBALL"
echo ">>> Code tarball uploaded to $CODE_TARBALL"

# ==============================================================================
# SPARK SUBMIT
# ==============================================================================

echo ""
echo ">>> Submitting SQL transformation job..."

spark-submit \
    --name "gsbkk_sql_${SQL_FILE##*/}_${LOG_DATE}" \
    --master yarn \
    --deploy-mode client \
    --driver-memory 5g \
    --driver-cores 2 \
    --executor-memory 4g \
    --num-executors 5 \
    --executor-cores 4 \
    --jars "$POSTGRES_JAR,$TRINO_JAR" \
    --archives "${PYTHON_ENV_TARBALL}#environment" \
    --py-files "$CODE_TARBALL" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.sql.parquet.writeLegacyFormat=true" \
    "$AIRFLOW_REPO/src/sql_runner.py" \
    "$@"

EXIT_CODE=$?

# ==============================================================================
# CLEANUP
# ==============================================================================

echo ""
echo ">>> Cleaning up..."
rm -rf "$LOCAL_ENV_DIR"
rm -rf "$TEMP_CREDS_DIR"

echo "========================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "SQL Runner: SUCCESS"
else
    echo "SQL Runner: FAILED (exit code: $EXIT_CODE)"
fi
echo "========================================================"

exit $EXIT_CODE
