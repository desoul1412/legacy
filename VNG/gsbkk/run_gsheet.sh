#!/usr/bin/env bash
################################################################################
# Google Sheets to RAW Shell Wrapper
################################################################################
#
# PURPOSE:
#   Read data from a Google Sheets worksheet and write raw JSON lines to HDFS.
#   Executes gsheet_to_raw.py directly (no Spark required) using the project's
#   Python virtual environment extracted from the HDFS archive.
#
# USAGE:
#   ./run_gsheet.sh key=value [key=value ...]
#
# REQUIRED ARGS:
#   sheet_id=<id>             Google Sheets document ID (from the URL)
#   worksheet=<name>          Worksheet (tab) name, e.g. "Daily Overall"
#   output_path=<hdfs_path>   HDFS destination; may contain {logDate} tokens
#   logDate=<date>            Date variable used in output_path substitution
#
# OPTIONAL ARGS:
#   cred_file=<filename>      Service-account JSON file in HDFS configs
#                             (default: tsn-data-0e06f020fc9b.json)
#   skip_empty=true           Skip all-empty rows (default: true)
#
# EXAMPLE:
#   ./run_gsheet.sh \
#       sheet_id=1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w \
#       worksheet="Daily Overall" \
#       output_path="hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/rfc/daily/{logDate}" \
#       logDate=2026-03-17
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
    echo "Usage: $0 sheet_id=<id> worksheet=<name> output_path=<hdfs_path> logDate=<date> [key=value ...]"
    echo ""
    echo "See script header for full argument reference."
    exit 1
fi

# Extract key args for the log header
SHEET_ID=""
WORKSHEET=""
LOG_DATE=""
OUTPUT_PATH=""
for arg in "$@"; do
    case "$arg" in
        sheet_id=*)   SHEET_ID="${arg#sheet_id=}" ;;
        worksheet=*)  WORKSHEET="${arg#worksheet=}" ;;
        logDate=*)    LOG_DATE="${arg#logDate=}" ;;
        output_path=*) OUTPUT_PATH="${arg#output_path=}" ;;
    esac
done

echo "========================================================"
echo "Google Sheets to RAW"
echo "========================================================"
echo "Sheet ID    : $SHEET_ID"
echo "Worksheet   : $WORKSHEET"
echo "Log Date    : $LOG_DATE"
echo "Output Path : $OUTPUT_PATH"
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
# Download service account key for Google Sheets API
hdfs dfs -get "$HDFS_CONFIGS/tsn-data-0e06f020fc9b.json" "$TEMP_CREDS_DIR/" 2>/dev/null \
    || echo "WARNING: tsn-data-0e06f020fc9b.json not found in HDFS"

export CREDENTIALS_DIR="$TEMP_CREDS_DIR"

echo ">>> Extracting Python environment..."
hdfs dfs -get "$PYTHON_ENV_TARBALL" "$LOCAL_ENV_DIR/environment.tar.gz"
tar -xzf "$LOCAL_ENV_DIR/environment.tar.gz" -C "$LOCAL_ENV_DIR"

PYTHON_BIN="$LOCAL_ENV_DIR/bin/python"
echo ">>> Python binary: $PYTHON_BIN"

# ==============================================================================
# RUN gsheet_to_raw.py
# ==============================================================================

echo ""
echo ">>> Running gsheet_to_raw.py ..."

"$PYTHON_BIN" "$AIRFLOW_REPO/src/gsheet_to_raw.py" "$@"

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
    echo "Google Sheets to RAW: SUCCESS"
else
    echo "Google Sheets to RAW: FAILED (exit code: $EXIT_CODE)"
fi
echo "========================================================"

exit $EXIT_CODE
