#!/usr/bin/env bash
################################################################################
# API Data Extraction Script
################################################################################
#
# PURPOSE:
#   Extract raw data from external APIs (SensorTower, Facebook, TikTok, etc.)
#   and store in HDFS as JSON files.
#
# USAGE:
#   ./run_api_extraction.sh <api_source> <endpoint> <config> <date>
#
# EXAMPLES:
#   # SensorTower - Top Games
#   ./run_api_extraction.sh sensortower top_games configs/api/sensortower.json 2024-12
#
#   # Facebook Ads
#   ./run_api_extraction.sh facebook ads_insights configs/api/facebook.json 2024-12-25
#
# SUPPORTED APIs:
#   - sensortower: Market research data
#   - facebook: Ads performance
#   - tiktok: Ads performance
#   - google_ads: Ads performance
#
# OUTPUT STRUCTURE:
#   hdfs://c0s/user/gsbkk-workspace-yc9t6/<api_source>/raw/<endpoint>/<date>/
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

# Proxy settings
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

# ==============================================================================
# ARGUMENT PARSING
# ==============================================================================

# Support both named and positional parameters
while [[ $# -gt 0 ]]; do
    case $1 in
        --apiSource)
            API_SOURCE="$2"
            shift 2
            ;;
        --endpoint)
            ENDPOINT="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --date)
            DATE="$2"
            shift 2
            ;;
        *)
            # Positional arguments (backward compatibility)
            if [ -z "$API_SOURCE" ]; then
                API_SOURCE="$1"
            elif [ -z "$ENDPOINT" ]; then
                ENDPOINT="$1"
            elif [ -z "$CONFIG_FILE" ]; then
                CONFIG_FILE="$1"
            elif [ -z "$DATE" ]; then
                DATE="$1"
            fi
            shift
            ;;
    esac
done

if [ -z "$API_SOURCE" ] || [ -z "$ENDPOINT" ] || [ -z "$CONFIG_FILE" ] || [ -z "$DATE" ]; then
    echo "Usage: $0 [--apiSource <source>] [--endpoint <endpoint>] [--config <config>] [--date <date>]"
    echo "   or: $0 <api_source> <endpoint> <config> <date>"
    echo ""
    echo "API Sources:"
    echo "  sensortower  - SensorTower market research API"
    echo "  facebook     - Facebook Ads API"
    echo "  tiktok       - TikTok Ads API"
    echo "  google_ads   - Google Ads API"
    echo ""
    echo "Examples:"
    echo "  $0 sensortower top_games configs/api/sensortower.json 2024-12"
    echo "  $0 --apiSource sensortower --endpoint top_games --config configs/api/sensortower.json --date 2024-12"
    echo "  $0 facebook ads_insights configs/api/facebook.json 2024-12-25"
    exit 1
fi

echo "========================================================"
echo "API Data Extraction"
echo "========================================================"
echo "API Source: $API_SOURCE"
echo "Endpoint: $ENDPOINT"
echo "Config: $CONFIG_FILE"
echo "Date: $DATE"
echo "========================================================"

# ==============================================================================
# SETUP LOCAL ENVIRONMENT
# ==============================================================================

LOCAL_ENV_DIR="/tmp/gsbkk_env_$$"
TEMP_CREDS_DIR="/tmp/gsbkk_creds_$$"

echo ">>> Setting up local environment..."
mkdir -p "$LOCAL_ENV_DIR"
mkdir -p "$TEMP_CREDS_DIR"

# Download credentials from HDFS
echo ">>> Downloading credentials from HDFS..."
hdfs dfs -get "$HDFS_CONFIGS/cred_gds.json" "$TEMP_CREDS_DIR/" 2>/dev/null || echo "GDS credentials not found"
hdfs dfs -get "$HDFS_CONFIGS/cred_trino.json" "$TEMP_CREDS_DIR/" 2>/dev/null || echo "Trino credentials not found"
hdfs dfs -get "$HDFS_CONFIGS/cred_tsn.json" "$TEMP_CREDS_DIR/" 2>/dev/null || echo "TSN credentials not found"

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
    echo ">>> Code tarball not found. Creating..."
    LOCAL_TARBALL="/tmp/gsbkk-src-$$.tar.gz"
    
    cd "$AIRFLOW_REPO"
    tar -czf "$LOCAL_TARBALL" \
        --exclude='*.pyc' \
        --exclude='__pycache__' \
        --exclude='.git' \
        --exclude='airflow/logs' \
        src/ configs/
    
    hdfs dfs -put -f "$LOCAL_TARBALL" "$CODE_TARBALL"
    rm -f "$LOCAL_TARBALL"
    echo ">>> Code tarball uploaded"
else
    echo ">>> Code tarball exists in HDFS"
fi

# ==============================================================================
# SPARK SUBMIT - API EXTRACTION
# ==============================================================================

echo ""
echo "========================================================"
echo "Running API Extraction"
echo "========================================================"

# Build spark-submit command
SPARK_ARGS=(
    --name "gsbkk_api_${API_SOURCE}_${ENDPOINT}_${DATE}"
    --master yarn
    --deploy-mode client
    --driver-memory 2g
    --driver-cores 1
    --executor-memory 2g
    --num-executors 2
    --executor-cores 2
    --archives "${PYTHON_ENV_TARBALL}#environment"
    --py-files "$CODE_TARBALL"
    --conf "spark.executorEnv.api_source=$API_SOURCE"
    --conf "spark.executorEnv.endpoint=$ENDPOINT"
    --conf "spark.sql.adaptive.enabled=true"
)

# Python script for API extraction
PYTHON_SCRIPT="$AIRFLOW_REPO/src/api_extractor.py"

# Build arguments
PYTHON_ARGS=(
    --api-source "$API_SOURCE"
    --endpoint "$ENDPOINT"
    --config "$CONFIG_FILE"
    --date "$DATE"
    --output "$HDFS_BASE/${API_SOURCE}/raw/${ENDPOINT}/${DATE}"
)

echo ">>> Spark Submit Command:"
echo "spark-submit ${SPARK_ARGS[@]} $PYTHON_SCRIPT ${PYTHON_ARGS[@]}"
echo ""

# Execute
spark-submit "${SPARK_ARGS[@]}" "$PYTHON_SCRIPT" "${PYTHON_ARGS[@]}"
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
    echo "API Extraction: SUCCESS"
    echo "Output: $HDFS_BASE/${API_SOURCE}/raw/${ENDPOINT}/${DATE}"
else
    echo "API Extraction: FAILED (exit code: $EXIT_CODE)"
fi
echo "========================================================"

exit $EXIT_CODE
