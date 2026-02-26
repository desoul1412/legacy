#!/bin/bash
# Game Health Check Pipeline - Full v3.0 Migration
# ETL → SQL Consolidation → Staging Tables
# Uses standardized VIP logic (total_rev / 25840) for all games
#
# Usage: ./run_game_health_pipeline.sh [log_date] [game_id] [report_type]
# 
# Examples:
#   ./run_game_health_pipeline.sh 2025-01-15                    # All games, all reports
#   ./run_game_health_pipeline.sh 2025-01-15 fw2               # Single game, all reports
#   ./run_game_health_pipeline.sh 2025-01-15 fw2 diagnostic    # Single game, one report

set -e  # Exit on error

LOG_DATE=${1:-$(date -d "yesterday" +%Y-%m-%d)}
GAME_FILTER=${2:-"all"}
REPORT_TYPE=${3:-"all"}  # all, diagnostic, package, server

# List of games to process (updated with l2m and skf)
GAMES=("cft" "fw2" "mlb" "mts" "td" "gnoth" "slth" "pwmsea" "cloudsongsea" "l2m" "skf")

# Filter games if specified
if [[ "$GAME_FILTER" != "all" ]]; then
    GAMES=("$GAME_FILTER")
fi

echo "=========================================="
echo "Game Health Check Pipeline (v3.0)"
echo "Date: $LOG_DATE"
echo "Games: ${GAMES[@]}"
echo "Report Type: $REPORT_TYPE"
echo "=========================================="

echo ""
echo "=== Phase 1: Extract Data from GDS Postgres ==="
echo ""

for game_id in "${GAMES[@]}"; do
    echo "[$game_id] Extracting data..."
    
    # Active details
    echo "  → active_details"
    ./run_etl_process.sh \
        --layout layouts/game_health_check/etl/active_details.json \
        --gameId $game_id \
        --logDate $LOG_DATE
    
    # Charge details
    echo "  → charge_details"
    ./run_etl_process.sh \
        --layout layouts/game_health_check/etl/charge_details.json \
        --gameId $game_id \
        --logDate $LOG_DATE
    
    # User profile
    echo "  → user_profile"
    ./run_etl_process.sh \
        --layout layouts/game_health_check/etl/user_profile.json \
        --gameId $game_id \
        --logDate $LOG_DATE
    
    # Campaign
    echo "  → campaign"
    ./run_etl_process.sh \
        --layout layouts/game_health_check/etl/campaign.json \
        --gameId $game_id \
        --logDate $LOG_DATE
    
    echo "  ✓ $game_id extraction complete"
done

# Extract currency mapping (shared across all games)
echo ""
echo "Extracting currency mapping..."
echo "Using common currency_mapping layout (shared HDFS path, no date partition)"
./run_etl_process.sh \
    --layout layouts/common/currency_mapping.json

echo ""
echo "=== Phase 2: SQL Consolidation ==="
echo ""

for game_id in "${GAMES[@]}"; do
    echo "[$game_id] Running SQL consolidation..."
    
    if [[ "$REPORT_TYPE" == "all" || "$REPORT_TYPE" == "diagnostic" ]]; then
        echo "  → diagnostic_daily"
        ./run_etl_process.sh \
            --layout layouts/game_health_check/cons/diagnostic_daily.json \
            --gameId $game_id \
            --logDate $LOG_DATE
    fi
    
    if [[ "$REPORT_TYPE" == "all" || "$REPORT_TYPE" == "package" ]]; then
        echo "  → package_performance"
        ./run_etl_process.sh \
            --layout layouts/game_health_check/cons/package_performance.json \
            --gameId $game_id \
            --logDate $LOG_DATE
    fi
    
    if [[ "$REPORT_TYPE" == "all" || "$REPORT_TYPE" == "server" ]]; then
        echo "  → server_performance"
        ./run_etl_process.sh \
            --layout layouts/game_health_check/cons/server_performance.json \
            --gameId $game_id \
            --logDate $LOG_DATE
    fi
    
    echo "  ✓ $game_id consolidation complete"
done

echo ""
echo "=========================================="
echo "✓ Game Health Check pipeline completed"
echo "=========================================="
echo ""
echo "Output tables in staging:"
if [[ "$REPORT_TYPE" == "all" || "$REPORT_TYPE" == "diagnostic" ]]; then
    for game_id in "${GAMES[@]}"; do
        echo "  - staging.ghc_diagnostic_daily_$game_id"
    done
fi
if [[ "$REPORT_TYPE" == "all" || "$REPORT_TYPE" == "package" ]]; then
    for game_id in "${GAMES[@]}"; do
        echo "  - staging.ghc_package_performance_$game_id"
    done
fi
if [[ "$REPORT_TYPE" == "all" || "$REPORT_TYPE" == "server" ]]; then
    for game_id in "${GAMES[@]}"; do
        echo "  - staging.ghc_server_performance_$game_id"
    done
fi
echo ""
echo "Note: VIP logic is standardized (total_rev / 25840) and embedded in SQL"
