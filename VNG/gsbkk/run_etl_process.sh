#!/usr/bin/env bash
################################################################################
# ETL Processing Script - Layout-based Transformations
################################################################################
#
# PURPOSE:
#   Process data using JSON layout files for ETL/STD/CONS transformations.
#   Handles data from company databases (Hadoop, Trino, Postgres).
#
# USAGE:
#   ./run_etl_process.sh <layouts> <date> [variables]
#
# EXAMPLES:
#   # SensorTower: Single layout
#   ./run_etl_process.sh layouts/sensortower/etl/top_games.json 2024-12
#
#   # SensorTower: Multiple layouts (comma-separated)
#   ./run_etl_process.sh "layouts/sensortower/etl/top_games.json,layouts/sensortower/etl/new_games.json" 2024-12
#
#   # Game Health Check: Process all stages
#   ./run_etl_process.sh "layouts/game_health_check/etl/daily_kpi.json,layouts/game_health_check/std/daily_kpi.json,layouts/game_health_check/cons/health_metrics.json" 2024-12-25
#
#   # Re-standardization: L2M with variables
#   ./run_etl_process.sh layouts/re-standardization/etl/l2m_recharge.json 2024-12-25 "game_id=l2m"
#
#   # Rolling Forecast: ETL stage only
#   ./run_etl_process.sh layouts/rolling_forecast/etl/calculations.json 2024-12-25
#
# LAYOUT STAGES (Organized by Project):
#   layouts/<project>/raw/  - Raw data definitions (for reference)
#   layouts/<project>/etl/  - Extract, transform, load (raw → columnar parquet)
#   layouts/<project>/std/  - Standardization (etl → standardized schema)
#   layouts/<project>/cons/ - Consolidation (std → final aggregated tables)
#
# PROJECTS:
#   sensortower         - Market research from SensorTower API
#   game_health_check   - Daily KPI monitoring for game health
#   rolling_forecast    - Revenue/DAU forecasting with Google Sheets
#   re-standardization  - Legacy data re-processing (L2M, etc.)
#
# DATA SOURCES:
#   - GDS Hadoop: hdfs://namenode-ha-2/user/hive/...
#   - GDS Trino: trino://trino-host:8080/hive/...
#   - GDS Postgres: jdbc:postgresql://gds-host:5432/...
#   - TSN Hadoop: hdfs://c0s/user/gsbkk-workspace-yc9t6/...
#
# OUTPUT:
#   - TSN Hadoop: hdfs://c0s/user/gsbkk-workspace-yc9t6/<project>/{etl,std,cons}/
#   - TSN Postgres: Database varies by project
#
################################################################################

set -e

# ==============================================================================
# ENVIRONMENT CONFIGURATION
# ==============================================================================

AIRFLOW_REPO=${AIRFLOW_REPO:="/opt/airflow/dags/repo"}
export AIRFLOW_HOME="/opt/airflow"
export HADOOP_CLIENT_OPTS="-Xmx2147483648 -Djava.net.preferIPv4Stack=true"
export HADOOP_CONF_DIR="/etc/hadoop"
export SPARK_HOME="/opt/spark"
export SPARK_CONF_DIR="/etc/spark"
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/hadoop/bin:/opt/hive/bin:/opt/spark/bin"
export PYTHONPATH="$PYTHONPATH:$AIRFLOW_REPO/src"

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
# ARGUMENT PARSING
# ==============================================================================

# Support both positional and named arguments
LAYOUTS=""
DATE=""
EXTRA_VARS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --layout)
            LAYOUTS="$2"
            shift 2
            ;;
        --logDate)
            DATE="$2"
            shift 2
            ;;
        --gameId)
            if [ -n "$EXTRA_VARS" ]; then
                EXTRA_VARS="$EXTRA_VARS,gameId=$2"
            else
                EXTRA_VARS="gameId=$2"
            fi
            shift 2
            ;;
        --logMonth)
            if [ -n "$EXTRA_VARS" ]; then
                EXTRA_VARS="$EXTRA_VARS,logMonth=$2"
            else
                EXTRA_VARS="logMonth=$2"
            fi
            shift 2
            ;;
        --vars)
            if [ -n "$EXTRA_VARS" ]; then
                EXTRA_VARS="$EXTRA_VARS,$2"
            else
                EXTRA_VARS="$2"
            fi
            shift 2
            ;;
        *)
            # Positional arguments (legacy format)
            if [ -z "$LAYOUTS" ]; then
                LAYOUTS="$1"
            elif [ -z "$DATE" ]; then
                DATE="$1"
            elif [ -z "$EXTRA_VARS" ]; then
                EXTRA_VARS="$1"
            fi
            shift
            ;;
    esac
done

if [ -z "$LAYOUTS" ] || [ -z "$DATE" ]; then
    echo "Usage: $0 <layouts> <date> [variables]"
    echo "   or: $0 --layout <layout> --logDate <date> [--gameId <game>] [--vars <vars>]"
    echo ""
    echo "Arguments:"
    echo "  layouts    - Layout file path or comma-separated list"
    echo "  date       - Processing date (YYYY-MM-DD or YYYY-MM)"
    echo "  variables  - Optional: Additional variables (key=value,key2=value2)"
    echo ""
    echo "Examples:"
    echo "  $0 layouts/sensortower/etl/top_games.json 2024-12"
    echo "  $0 --layout layouts/sensortower/etl/top_games.json --logDate 2024-12"
    echo "  $0 --layout layouts/re-standardization/etl/l2m_recharge.json --logDate 2024-12-25 --gameId l2m"
    exit 1
fi

echo "========================================================"
echo "ETL Processing"
echo "========================================================"
echo "Layouts: $LAYOUTS"
echo "Date: $DATE"
[ -n "$EXTRA_VARS" ] && echo "Extra Variables: $EXTRA_VARS"
echo "========================================================"

# ==============================================================================
# SETUP LOCAL ENVIRONMENT
# ==============================================================================

LOCAL_ENV_DIR="/tmp/gsbkk_env_$$"
TEMP_CREDS_DIR="/tmp/gsbkk_creds_$$"

echo ">>> Setting up local environment..."
mkdir -p "$LOCAL_ENV_DIR"
mkdir -p "$TEMP_CREDS_DIR"

# Download all credentials
echo ">>> Downloading credentials from HDFS..."
hdfs dfs -get "$HDFS_CONFIGS/cred_gds.json" "$TEMP_CREDS_DIR/" 2>/dev/null || echo "GDS credentials not found"
hdfs dfs -get "$HDFS_CONFIGS/cred_trino.json" "$TEMP_CREDS_DIR/" 2>/dev/null || echo "Trino credentials not found"
hdfs dfs -get "$HDFS_CONFIGS/cred_tsn.json" "$TEMP_CREDS_DIR/" 2>/dev/null || echo "TSN credentials not found"
hdfs dfs -get "$HDFS_CONFIGS/tsn-data-0e06f020fc9b.json" "$TEMP_CREDS_DIR/" 2>/dev/null || echo "Google Sheets credentials not found"

# Create expected filename for Google Sheets credentials
if [ -f "$TEMP_CREDS_DIR/tsn-data-0e06f020fc9b.json" ]; then
    cp "$TEMP_CREDS_DIR/tsn-data-0e06f020fc9b.json" "$TEMP_CREDS_DIR/gsheet_creds.json"
    echo ">>> Google Sheets credentials copied to gsheet_creds.json"
fi

export CREDENTIALS_DIR="$TEMP_CREDS_DIR"

# Extract Python environment for driver
echo ">>> Extracting Python environment..."
hdfs dfs -get "$PYTHON_ENV_TARBALL" "$LOCAL_ENV_DIR/environment.tar.gz"
tar -xzf "$LOCAL_ENV_DIR/environment.tar.gz" -C "$LOCAL_ENV_DIR"

# Set Python paths
# Driver uses locally extracted environment (client mode)
export PYSPARK_DRIVER_PYTHON="$LOCAL_ENV_DIR/bin/python"
# Executors use environment from --archives
export PYSPARK_PYTHON="./environment/bin/python"

# ==============================================================================
# CODE TARBALL MANAGEMENT
# ==============================================================================

echo ">>> Checking code tarball..."
if ! hdfs dfs -test -f "$CODE_TARBALL"; then
    echo ">>> Creating code tarball..."
    LOCAL_TARBALL="/tmp/gsbkk-src-$$.tar.gz"
    cd "$AIRFLOW_REPO"
    tar -czf "$LOCAL_TARBALL" \
        --exclude='*.pyc' \
        --exclude='__pycache__' \
        --exclude='.git' \
        --exclude='airflow/logs' \
        src/ configs/ layouts/
    hdfs dfs -put -f "$LOCAL_TARBALL" "$CODE_TARBALL"
    rm -f "$LOCAL_TARBALL"
fi

# ==============================================================================
# SPARK SUBMIT - ETL PROCESSING
# ==============================================================================

echo ""
echo "========================================================"
echo "Processing Layouts"
echo "========================================================"

# Parse comma-separated layouts
IFS=',' read -ra LAYOUT_ARRAY <<< "$LAYOUTS"

# Process each layout
EXIT_CODE=0
for LAYOUT in "${LAYOUT_ARRAY[@]}"; do
    LAYOUT=$(echo "$LAYOUT" | xargs)  # Trim whitespace
    
    echo ""
    echo ">>> Processing layout: $LAYOUT"
    
    # Build Spark arguments
    SPARK_ARGS=(
        --name "gsbkk_etl_${LAYOUT##*/}_${DATE}"
        --master yarn
        --deploy-mode client
        --driver-memory 5g
        --driver-cores 2
        --executor-memory 4g
        --num-executors 5
        --executor-cores 4
        --jars "$POSTGRES_JAR,$TRINO_JAR"
        --archives "${PYTHON_ENV_TARBALL}#environment"
        --py-files "$CODE_TARBALL"
        --conf "spark.sql.adaptive.enabled=true"
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true"
        --conf "spark.sql.broadcastTimeout=600"
        --conf "spark.sql.autoBroadcastJoinThreshold=52428800"
    )
    
    PYTHON_SCRIPT="$AIRFLOW_REPO/src/etl_engine.py"
    
    # Build variables string
    VARS_STRING="logDate=$DATE"
    if [ -n "$EXTRA_VARS" ]; then
        VARS_STRING="$VARS_STRING,$EXTRA_VARS"
    fi
    
    PYTHON_ARGS=(
        --layout "$LAYOUT"
        --vars "$VARS_STRING"
    )
    
    echo ">>> Spark Submit Command:"
    echo "spark-submit ${SPARK_ARGS[@]} $PYTHON_SCRIPT ${PYTHON_ARGS[@]}"
    echo ""
    
    # Execute
    spark-submit "${SPARK_ARGS[@]}" "$PYTHON_SCRIPT" "${PYTHON_ARGS[@]}"
    
    LAYOUT_EXIT_CODE=$?
    if [ $LAYOUT_EXIT_CODE -ne 0 ]; then
        echo ">>> ERROR: Layout $LAYOUT failed with exit code $LAYOUT_EXIT_CODE"
        EXIT_CODE=$LAYOUT_EXIT_CODE
        break
    fi
    
    echo ">>> Layout $LAYOUT completed successfully"
done

# ==============================================================================
# CLEANUP
# ==============================================================================

echo ""
echo ">>> Cleaning up..."
rm -rf "$LOCAL_ENV_DIR"
rm -rf "$TEMP_CREDS_DIR"

echo "========================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "ETL Processing: SUCCESS"
    echo "Processed ${#LAYOUT_ARRAY[@]} layout(s)"
else
    echo "ETL Processing: FAILED (exit code: $EXIT_CODE)"
fi
echo "========================================================"

exit $EXIT_CODE
