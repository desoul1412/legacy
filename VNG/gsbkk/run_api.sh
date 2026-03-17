#!/usr/bin/env bash
################################################################################
# API to RAW Shell Wrapper
################################################################################
#
# PURPOSE:
#   Fetch data from an external HTTP API and write raw JSON lines to HDFS.
#   Executes api_to_raw.py directly (no Spark required) using the project's
#   Python virtual environment extracted from the HDFS archive.
#
# USAGE:
#   ./run_api.sh key=value [key=value ...]
#
# REQUIRED ARGS:
#   url_template=<url>        API URL; may contain {{ logDate }} Jinja2 placeholders
#   output_path=<hdfs_path>   HDFS destination; may contain {logDate} tokens
#   logDate=<date>            Date variable (YYYY-MM-DD or YYYY-MM)
#
# OPTIONAL ARGS:
#   params='{"k":"v"}'        Extra query params as a JSON object
#   cred_file=cred_tsn.json   Credential filename in HDFS configs directory
#   cred_key=sensortower      Key within the credential file
#   auth_param=auth_token     Query param name for the token (default: auth_token)
#   auth_header=Authorization Header name for the token (alternative to auth_param)
#   auth_value_key=api_key    Key in the credentials dict that holds the token
#   response_key=ids          Dot-path into the JSON response to the data list
#   paginate=false            Enable offset-based pagination (default: false)
#   page_param=offset         Offset param name for pagination (default: offset)
#   page_size=10000           Records per page (default: 10000)
#   method=GET                HTTP method: GET or POST (default: GET)
#   timeout=60                Request timeout in seconds (default: 60)
#   max_retries=3             Max retry attempts on rate-limit / server errors
#
# EXAMPLE:
#   ./run_api.sh \
#       url_template="https://api.sensortower.com/v1/ios/apps/app_ids?category=6014&start_date={{ logDate }}&limit=10000" \
#       cred_file=cred_tsn.json \
#       cred_key=sensortower \
#       response_key=ids \
#       paginate=true \
#       output_path="hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/sensortower/new_games_ios/{logDate}" \
#       logDate=2024-01-15
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

# ==============================================================================
# VALIDATE ARGS
# ==============================================================================

if [ $# -lt 1 ]; then
    echo "Usage: $0 url_template=<url> output_path=<hdfs_path> logDate=<date> [key=value ...]"
    echo ""
    echo "See script header for full argument reference."
    exit 1
fi

# Extract key args for the log header
URL_TEMPLATE=""
LOG_DATE=""
OUTPUT_PATH=""
for arg in "$@"; do
    case "$arg" in
        url_template=*) URL_TEMPLATE="${arg#url_template=}" ;;
        logDate=*)      LOG_DATE="${arg#logDate=}" ;;
        output_path=*)  OUTPUT_PATH="${arg#output_path=}" ;;
    esac
done

echo "========================================================"
echo "API to RAW"
echo "========================================================"
echo "URL Template : $URL_TEMPLATE"
echo "Log Date     : $LOG_DATE"
echo "Output Path  : $OUTPUT_PATH"
echo "========================================================"

# ==============================================================================
# SETUP LOCAL PYTHON ENVIRONMENT
# ==============================================================================

LOCAL_ENV_DIR="/tmp/gsbkk_env_$$"
TEMP_CREDS_DIR="/tmp/gsbkk_creds_$$"

mkdir -p "$LOCAL_ENV_DIR"
mkdir -p "$TEMP_CREDS_DIR"

echo ""
echo ">>> Downloading credentials from HDFS..."
hdfs dfs -get "$HDFS_CONFIGS/cred_tsn.json" "$TEMP_CREDS_DIR/" 2>/dev/null \
    || echo "WARNING: cred_tsn.json not found in HDFS"
hdfs dfs -get "$HDFS_CONFIGS/cred_gds.json" "$TEMP_CREDS_DIR/" 2>/dev/null \
    || echo "WARNING: cred_gds.json not found in HDFS"

export CREDENTIALS_DIR="$TEMP_CREDS_DIR"

echo ">>> Extracting Python environment..."
hdfs dfs -get "$PYTHON_ENV_TARBALL" "$LOCAL_ENV_DIR/environment.tar.gz"
tar -xzf "$LOCAL_ENV_DIR/environment.tar.gz" -C "$LOCAL_ENV_DIR"

PYTHON_BIN="$LOCAL_ENV_DIR/bin/python"
echo ">>> Python binary: $PYTHON_BIN"

# ==============================================================================
# RUN api_to_raw.py
# ==============================================================================

echo ""
echo ">>> Running api_to_raw.py ..."

"$PYTHON_BIN" "$AIRFLOW_REPO/src/api_to_raw.py" "$@"

EXIT_CODE=$?

# ==============================================================================
# CLEANUP
# ==============================================================================

echo ""
echo ">>> Cleaning up ..."
rm -rf "$LOCAL_ENV_DIR"
rm -rf "$TEMP_CREDS_DIR"

echo "========================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "API to RAW: SUCCESS"
else
    echo "API to RAW: FAILED (exit code: $EXIT_CODE)"
fi
echo "========================================================"

exit $EXIT_CODE
