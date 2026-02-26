#!/bin/bash
set -e  # Exit on error

GAME_ID=$1
LOG_DATE=$2
MARKET_TYPE=$3

# Validate inputs
if [ -z "$GAME_ID" ] || [ -z "$LOG_DATE" ]; then
    echo "Usage: $0 <game_id> <log_date> [market_type]"
    exit 1
fi

# Auto-detect market type if not provided
if [ -z "$MARKET_TYPE" ]; then
    case $GAME_ID in
        slth|gnoth)
            MARKET_TYPE="single"
            ;;
        pwmsea|omgthai)
            MARKET_TYPE="sea"
            ;;
        cft|fw2|mlb|mts|cloudsongsea|td|l2m)
            MARKET_TYPE="multi"
            ;;
        *)
            echo "Unknown game_id: $GAME_ID. Please specify market_type."
            exit 1
            ;;
    esac
fi

case $MARKET_TYPE in
    single)
        ./run_etl_process.sh \
            --layout layouts/rolling_forecast/cons/single_market.json \
            --gameId $GAME_ID \
            --logDate $LOG_DATE
        ;;
    sea)
        ./run_etl_process.sh \
            --layout layouts/rolling_forecast/cons/sea_market.json \
            --gameId $GAME_ID \
            --logDate $LOG_DATE
        ;;
    multi)
        ./run_etl_process.sh \
            --layout layouts/rolling_forecast/cons/multi_country.json \
            --gameId $GAME_ID \
            --logDate $LOG_DATE
        ;;
    *)
        echo "Invalid market type: $MARKET_TYPE"
        exit 1
        ;;
esac

AIRFLOW_REPO=${AIRFLOW_REPO:="/opt/airflow/dags/repo"}
cd "$AIRFLOW_REPO"
python3 src/pipelines/rolling_forecast/rfc_gsheet_writer.py $GAME_ID $LOG_DATE
