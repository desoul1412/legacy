#!/usr/bin/env bash
################################################################################
# GSBKK Pipeline Executor - Unified Entry Point
################################################################################
#
# PURPOSE:
#   Unified bash script that executes both pipeline-based and layout-based
#   data processing workflows on Spark/YARN using tarball deployment method.
#
# ARCHITECTURE:
#   This script is the single entry point for all GSBKK pipelines, routing
#   requests to either:
#   - etl_engine.py (for layout-based ETL/STD/CONS transformations)
#   - run_pipeline.py (for complex pipeline workflows)
#
# EXECUTION MODELS:
#   1. Layout-based: Process JSON layout files with standardized transformations
#      Example: ./run_pipeline.sh etl "layouts/sensortower/etl/top_games.json" 2024-12
#
#   2. Pipeline-based: Execute complex workflows with custom logic
#      Example: ./run_pipeline.sh market_research extract_top_games 2024-12
#
# KEY RESPONSIBILITIES:
#   - Parse action type and route to appropriate Python script
#   - Set up HDFS environment (credentials, code tarball, Python environment)
#   - Build and execute spark-submit command
#   - Handle cleanup and error codes
#
# DEPLOYMENT METHOD: Tarball
#   - Code: Uploaded to HDFS as gsbkk-src.tar.gz
#   - Python env: Pre-built environment.tar.gz in HDFS
#   - Credentials: JSON files in HDFS (downloaded to local temp dir)
#   - JDBC drivers: PostgreSQL JAR in HDFS
#
# HDFS STRUCTURE:
#   hdfs://c0s/user/gsbkk-workspace-yc9t6/
#   ├── archives/environment.tar.gz (Python venv)
#   ├── archives/gsbkk-src.tar.gz (Code tarball)
#   ├── configs/cred_*.json (Credentials)
#   └── postgresql-42.7.4.jar (JDBC driver)
#
# SPARK CONFIGURATION:
#   - Mode: Client (driver runs locally on Airflow node)
#   - Resources: 5 executors × 4 cores × 4GB memory
#   - Archives: Python environment extracted to ./environment on executors
#   - Py-files: Code distributed via --py-files tarball
#
# AUTHOR: GSBKK Team
# VERSION: 2.3.0
# LAST UPDATED: December 2024
#
################################################################################

set -e

# ==============================================================================
# ENVIRONMENT CONFIGURATION
# ==============================================================================
# Set up environment variables to match company Airflow infrastructure.
# These paths and settings are pre-configured in the production environment.

# Airflow paths (company-provided)
AIRFLOW_REPO=${AIRFLOW_REPO:="/opt/airflow/dags/repo"}
export AIRFLOW_HOME="/opt/airflow"

# Hadoop configuration
# - HADOOP_CLIENT_OPTS: JVM settings for Hadoop client operations
# - HADOOP_CONF_DIR: Location of Hadoop configuration files (core-site.xml, hdfs-site.xml)
export HADOOP_CLIENT_OPTS="-Xmx2147483648 -Djava.net.preferIPv4Stack=true"
export HADOOP_CONF_DIR="/etc/hadoop"

# Spark configuration
# - SPARK_HOME: Spark installation directory
# - SPARK_CONF_DIR: Spark configuration files (spark-defaults.conf, etc.)
# - PYTHONSTARTUP: Python initialization script for PySpark shell
export SPARK_HOME="/opt/spark"
export SPARK_CONF_DIR="/etc/spark"
export PYTHONSTARTUP="/opt/spark/python/pyspark/shell.py"

# Path configuration
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/hadoop/bin:/opt/hive/bin:/opt/spark/bin"
export PYTHONPATH="$PYTHONPATH:$AIRFLOW_REPO/src"

# Proxy settings (if needed)
export HTTP_PROXY="http://proxy.dp.vng.vn:3128"
export HTTPS_PROXY="http://proxy.dp.vng.vn:3128"

# ==============================================================================
# HDFS PATHS CONFIGURATION
# ==============================================================================
# Define all HDFS paths used by the pipeline for code, data, and dependencies.
# All paths are under the workspace directory allocated by the platform team.

# Base workspace directory (allocated by platform team)
HDFS_BASE="hdfs://c0s/user/gsbkk-workspace-yc9t6"
HDFS_ARCHIVES="$HDFS_BASE/archives"
HDFS_CONFIGS="$HDFS_BASE/configs"
HDFS_JARS="$HDFS_BASE"

# Python environment tarball
# Pre-built virtualenv with pandas, pyspark, requests, etc.
# Built once and reused across all pipeline executions
PYTHON_ENV_TARBALL="$HDFS_ARCHIVES/environment.tar.gz"

# Code tarball (created locally, uploaded to HDFS)
# Contains src/, configs/ directories from this repository
# Recreated on each execution to include latest code changes
CODE_TARBALL="$HDFS_ARCHIVES/gsbkk-src.tar.gz"

# PostgreSQL JDBC driver
# Used for writing to TSN PostgreSQL database
POSTGRES_JAR="$HDFS_JARS/postgresql-42.7.4.jar"

# Credentials directory (contains separate JSON files)
# Downloaded to local temp directory during execution
# Files: cred_gds.json, cred_trino.json, cred_tsn.json
HDFS_CONFIGS="$HDFS_BASE/configs"

# ==============================================================================
# PARSE ARGUMENTS
# ==============================================================================

PIPELINE_TYPE="$1"
shift

# Check for ETL-style actions with layouts
LAYOUTS=""
if [ "$PIPELINE_TYPE" = "etl" ] || [ "$PIPELINE_TYPE" = "extract" ] || [ "$PIPELINE_TYPE" = "process" ]; then
    # ETL action: etl <game_id> <layouts> <date>
    # Or could be: etl <layouts> <date> (if no game_id needed)
    GAME_ID_OR_LAYOUTS="$1"
    shift
    
    # Check if next arg looks like a layout path or a date
    if [[ "$1" =~ ^[0-9]{4}-[0-9]{2} ]]; then
        # Pattern is: etl <layouts> <date>
        LAYOUTS="$GAME_ID_OR_LAYOUTS"
        PIPELINE_ARG=""
        LOG_DATE="$1"
        shift
    else
        # Pattern is: etl <game_id> <layouts> <date>
        PIPELINE_ARG="$GAME_ID_OR_LAYOUTS"  # This is game_id
        LAYOUTS="$1"
        shift
        LOG_DATE="$1"
        shift
    fi
    STEP_ARG=""
    EXTRA_ARGS="$@"
elif [ "$PIPELINE_TYPE" = "market_research" ]; then
    # Check if next arg is a known step
    case "$1" in
        extract_top_games|extract_new_games|extract_metadata|extract_performance|process_etl|process_std|write_postgres|all)
            STEP_ARG="$1"
            shift
            ;;
        *)
            # Not a step, treat as date
            ;;
    esac
fi

# Optional: config file or game_id depending on pipeline
# For market_research with step, this might be the date
PIPELINE_ARG="$1"
shift || true

# Date argument (YYYY-MM-DD or YYYY-MM format)
# For market_research with step, date might be in PIPELINE_ARG
if [ -n "$STEP_ARG" ]; then
    # market_research with step: PIPELINE_ARG is actually the date
    LOG_DATE="$PIPELINE_ARG"
    PIPELINE_ARG="configs/pipelines/market_research.json"  # Default config
else
    LOG_DATE="$1"
    shift || true
fi

# Remaining arguments
EXTRA_ARGS="$@"

if [ -z "$PIPELINE_TYPE" ] || [ -z "$LOG_DATE" ]; then
    echo "Usage: $0 <action_type> [step|layouts] [config_or_game_id] <date> [extra_args]"
    echo ""
    echo "Action types:"
    echo "  ETL/Layout-based actions:"
    echo "    etl      - Process ETL layouts (raw to parquet)"
    echo "    extract  - Same as etl"
    echo "    process  - Same as etl"
    echo ""
    echo "  Pipeline-based actions:"
    echo "    market_research  - SensorTower API data extraction"
    echo "    game_health      - Game health diagnostics"
    echo "    rolling_forecast - Rolling forecast (daily/monthly)"
    echo "    re_std           - Re-standardization"
    echo ""
    echo "Market Research Steps (optional):"
    echo "  all                  - Run all steps (default)"
    echo "  extract_top_games    - Extract top games by country"
    echo "  extract_new_games    - Extract newly released games"
    echo "  extract_metadata     - Extract app metadata"
    echo "  extract_performance  - Extract performance metrics"
    echo "  process_etl          - Convert raw to parquet"
    echo "  process_std          - Standardize data"
    echo "  write_postgres       - Write to PostgreSQL"
    echo ""
    echo "Examples:"
    echo "  # ETL with layouts"
    echo "  $0 etl layouts/sensortower/etl/top_games.json 2024-12"
    echo "  $0 etl \"etl/top_games.json,etl/new_games.json\" 2024-12"
    echo ""
    echo "  # Market research pipeline"
    echo "  $0 market_research 2024-12                          # Run all steps"
    echo "  $0 market_research extract_top_games 2024-12        # Run one step"
    echo ""
    echo "  # Other pipelines"
    echo "  $0 game_health l2m 2024-12-24"
    echo "  $0 rolling_forecast l2m 2024-12-24 daily"
    echo "  $0 re_std configs/pipelines/re_std_l2m_roles.json 2024-12-24"
    exit 1
fi

# ==============================================================================
# SETUP DRIVER ENVIRONMENT (THREAD-SAFE)
# ==============================================================================

echo "========================================================"
echo "GSBKK Pipeline Executor - Tarball Method"
echo "========================================================"
echo "Pipeline Type:     $PIPELINE_TYPE"
if [ -n "$STEP_ARG" ]; then
    echo "Pipeline Step:     $STEP_ARG"
fi
echo "Pipeline Arg:      $PIPELINE_ARG"
echo "Log Date:          $LOG_DATE"
echo "Extra Args:        $EXTRA_ARGS"
echo "========================================================"

echo ""
echo ">>> Setting up local Python environment for Driver..."

# Create GUARANTEED UNIQUE local directory to prevent race conditions
LOCAL_ENV_DIR=$(mktemp -d /tmp/driver_env_XXXXXX)

# Download the tarball from HDFS
echo ">>> Downloading Python environment from HDFS..."
hdfs dfs -get "$PYTHON_ENV_TARBALL" "$LOCAL_ENV_DIR/env.tar.gz"

# Extract
echo ">>> Extracting Python environment..."
tar -xzf "$LOCAL_ENV_DIR/env.tar.gz" -C "$LOCAL_ENV_DIR"

# Point PYSPARK_DRIVER_PYTHON to local extracted environment
export PYSPARK_DRIVER_PYTHON="$LOCAL_ENV_DIR/bin/python"

# Executor Python (uses the #environment alias from --archives)
export PYSPARK_PYTHON="./environment/bin/python"

echo ">>> Driver Python:   $PYSPARK_DRIVER_PYTHON"
echo ">>> Executor Python: $PYSPARK_PYTHON"

# ==============================================================================
# DOWNLOAD CREDENTIALS FROM HDFS
# ==============================================================================

echo ""
echo ">>> Downloading credentials from HDFS..."

# Create temp directory for credentials
TEMP_CREDS_DIR="/tmp/gsbkk_creds_$$"
mkdir -p "$TEMP_CREDS_DIR"

# Download all credential files
CRED_FILES=("cred_tsn.json" "cred_gds.json" "cred_trino.json" "tsn-data-0e06f020fc9b.json")
DOWNLOADED_COUNT=0

for cred_file in "${CRED_FILES[@]}"; do
    hdfs dfs -get "$HDFS_CONFIGS/$cred_file" "$TEMP_CREDS_DIR/" 2>/dev/null && {
        echo ">>> Downloaded: $cred_file"
        ((DOWNLOADED_COUNT++))
    } || {
        echo "⚠️  Could not download: $cred_file (will try local or env vars)"
    }
done

if [ $DOWNLOADED_COUNT -gt 0 ]; then
    export CREDENTIALS_DIR="$TEMP_CREDS_DIR"
    echo ">>> Credentials directory: $TEMP_CREDS_DIR"
    echo ">>> Successfully downloaded $DOWNLOADED_COUNT credential file(s)"
else
    echo "WARNING: No credentials downloaded from HDFS"
    echo "Attempting to use local credentials or environment variables"
fi

# ==============================================================================
# BUILD CODE TARBALL (IF NOT EXISTS IN HDFS)
# ==============================================================================

echo ""
echo ">>> Checking for code tarball in HDFS..."

# Check if tarball exists in HDFS
if ! hdfs dfs -test -e "$CODE_TARBALL"; then
    echo ">>> Code tarball not found in HDFS. Building..."
    
    # Build tarball locally
    LOCAL_TARBALL="/tmp/gsbkk-src-$$.tar.gz"
    cd "$AIRFLOW_REPO"
    tar -czf "$LOCAL_TARBALL" \
        --exclude='*.pyc' \
        --exclude='__pycache__' \
        --exclude='.git' \
        --exclude='dist' \
        --exclude='airflow/logs' \
        --exclude='*.md' \
        src/ configs/
    
    # Upload to HDFS
    echo ">>> Uploading code tarball to HDFS..."
    hdfs dfs -put -f "$LOCAL_TARBALL" "$CODE_TARBALL"
    
    # Cleanup local tarball
    rm -f "$LOCAL_TARBALL"
    
    echo ">>> Code tarball uploaded to: $CODE_TARBALL"
else
    echo ">>> Code tarball found in HDFS: $CODE_TARBALL"
fi

# ==============================================================================
# SPARK SUBMIT
# ==============================================================================

echo ""
echo "========================================================"
echo "Running Spark Pipeline"
echo "========================================================"

# Build spark-submit command arguments
SPARK_ARGS=(
    --name "gsbkk_${PIPELINE_TYPE}_${LOG_DATE}"
    --master yarn
    --deploy-mode client
    --driver-memory 5g
    --driver-cores 2
    --executor-memory 4g
    --num-executors 5
    --executor-cores 4
    --jars "$POSTGRES_JAR"
    --archives "${PYTHON_ENV_TARBALL}#environment"
    --py-files "$CODE_TARBALL"
    --conf "spark.executorEnv.pipeline_type=$PIPELINE_TYPE"
    --conf "spark.executorEnv.log_date=$LOG_DATE"
    --conf "spark.sql.adaptive.enabled=true"
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true"
)

# Determine which script to run and build arguments based on action type
if [ "$PIPELINE_TYPE" = "etl" ] || [ "$PIPELINE_TYPE" = "extract" ] || [ "$PIPELINE_TYPE" = "process" ]; then
    # For ETL-style actions, process each layout separately
    PIPELINE_SCRIPT="$AIRFLOW_REPO/src/etl_engine.py"
    
    # Split layouts by comma and process each one
    IFS=',' read -ra LAYOUT_ARRAY <<< "$LAYOUTS"
    
    for LAYOUT in "${LAYOUT_ARRAY[@]}"; do
        LAYOUT=$(echo "$LAYOUT" | xargs)  # Trim whitespace
        
        echo ">>> Processing layout: $LAYOUT"
        
        # Build arguments for etl_engine.py
        PYTHON_ARGS=(
            --layout "$LAYOUT"
            --vars "logDate=$LOG_DATE"
        )
        
        # Add gameId if provided
        if [ -n "$PIPELINE_ARG" ]; then
            PYTHON_ARGS+=(--vars "gameId=$PIPELINE_ARG")
        fi
        
        echo ">>> Spark Submit Command:"
        echo "spark-submit ${SPARK_ARGS[@]} $PIPELINE_SCRIPT ${PYTHON_ARGS[@]}"
        echo ""
        
        # Execute spark-submit for this layout
        spark-submit "${SPARK_ARGS[@]}" "$PIPELINE_SCRIPT" "${PYTHON_ARGS[@]}"
        
        # Check if this layout failed
        LAYOUT_EXIT_CODE=$?
        if [ $LAYOUT_EXIT_CODE -ne 0 ]; then
            echo ">>> ERROR: Layout $LAYOUT failed with exit code $LAYOUT_EXIT_CODE"
            EXIT_CODE=$LAYOUT_EXIT_CODE
            break
        fi
    done
    
    # Skip the normal pipeline execution below
    SKIP_NORMAL_EXECUTION=true
else
    # For regular pipeline actions, use run_pipeline.py
    PIPELINE_SCRIPT="$AIRFLOW_REPO/src/run_pipeline.py"
    
    # Build arguments for the Python script
    PYTHON_ARGS=(
        "$PIPELINE_TYPE"
        --config "$PIPELINE_ARG"
        --date "$LOG_DATE"
    )
    
    # Add step parameter for market_research
    if [ -n "$STEP_ARG" ]; then
        PYTHON_ARGS+=(--step "$STEP_ARG")
    fi
    
    SKIP_NORMAL_EXECUTION=false
fi

# Add extra arguments
if [ -n "$EXTRA_ARGS" ]; then
    PYTHON_ARGS+=($EXTRA_ARGS)
fi

# Execute spark-submit (only for non-ETL actions, as ETL already executed above)
if [ "$SKIP_NORMAL_EXECUTION" = false ]; then
    echo ">>> Spark Submit Command:"
    echo "spark-submit ${SPARK_ARGS[@]} $PIPELINE_SCRIPT ${PYTHON_ARGS[@]}"
    echo ""
    
    # Execute spark-submit
    spark-submit "${SPARK_ARGS[@]}" "$PIPELINE_SCRIPT" "${PYTHON_ARGS[@]}"
    
    # Capture exit code
    EXIT_CODE=$?
fi

# ==============================================================================
# CLEANUP
# ==============================================================================

echo ""
echo "========================================================"
echo "Cleaning Up"
echo "========================================================"

echo ">>> Removing local driver environment..."
rm -rf "$LOCAL_ENV_DIR"

echo ">>> Removing temporary credentials..."
rm -rf "$TEMP_CREDS_DIR"

echo "========================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Pipeline Execution: SUCCESS"
else
    echo "Pipeline Execution: FAILED (exit code: $EXIT_CODE)"
fi
echo "========================================================"

# Exit with spark-submit's exit code so Airflow knows the status
exit $EXIT_CODE
