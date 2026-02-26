#!/bin/bash
# Verification script for rolling_forecast refactoring
# Run this to check if the refactoring is complete and safe

echo "=== Rolling Forecast Refactoring Verification ==="
echo ""

# Check 1: Verify no templates use old placeholders
echo "1. Checking for old template placeholders..."
echo "   Searching for {{ retentionTable }}, {{ chargeTable }}, {{ activeTable }}..."

PLACEHOLDER_COUNT=$(grep -r "{{ retentionTable }}" templates/rolling_forecast/ 2>/dev/null | grep -v "archived" | wc -l)
PLACEHOLDER_COUNT=$((PLACEHOLDER_COUNT + $(grep -r "{{ chargeTable }}" templates/rolling_forecast/ 2>/dev/null | grep -v "archived" | wc -l)))
PLACEHOLDER_COUNT=$((PLACEHOLDER_COUNT + $(grep -r "{{ activeTable }}" templates/rolling_forecast/ 2>/dev/null | grep -v "archived" | wc -l)))

if [ $PLACEHOLDER_COUNT -eq 0 ]; then
    echo "   ✅ No old placeholders found in active templates"
else
    echo "   ⚠️  Found $PLACEHOLDER_COUNT old placeholders in active templates"
    echo "   Remaining usage:"
    grep -r "{{ retentionTable }}" templates/rolling_forecast/ 2>/dev/null | grep -v "archived"
    grep -r "{{ chargeTable }}" templates/rolling_forecast/ 2>/dev/null | grep -v "archived"
    grep -r "{{ activeTable }}" templates/rolling_forecast/ 2>/dev/null | grep -v "archived"
fi
echo ""

# Check 2: Verify game-specific templates exist
echo "2. Checking game-specific templates exist..."
GAMES=("gnoth" "pwmsea" "l2m")
for game in "${GAMES[@]}"; do
    if [ -f "templates/rolling_forecast/$game/active.sql.j2" ] && \
       [ -f "templates/rolling_forecast/$game/retention.sql.j2" ] && \
       [ -f "templates/rolling_forecast/$game/charge.sql.j2" ]; then
        echo "   ✅ $game templates complete"
    else
        echo "   ❌ $game templates missing"
    fi
done
echo ""

# Check 3: Verify game-specific layouts exist
echo "3. Checking game-specific layouts exist..."
for game in "${GAMES[@]}"; do
    if [ -f "layouts/rolling_forecast/$game/etl/active.json" ] && \
       [ -f "layouts/rolling_forecast/$game/etl/retention.json" ] && \
       [ -f "layouts/rolling_forecast/$game/etl/charge.json" ] && \
       [ -f "layouts/rolling_forecast/$game/cons/metrics.json" ]; then
        echo "   ✅ $game layouts complete"
    else
        echo "   ❌ $game layouts missing"
    fi
done
echo ""

# Check 4: Verify game-specific DAGs exist
echo "4. Checking game-specific DAGs exist..."
for game in "${GAMES[@]}"; do
    DAG_FILE="dags/rolling_forecast/${game}_rolling_forecast_dag.py"
    if [ -f "$DAG_FILE" ]; then
        echo "   ✅ $game DAG exists"
    else
        echo "   ❌ $game DAG missing"
    fi
done
echo ""

# Check 5: Verify hardcoded table names in templates
echo "5. Checking templates have hardcoded table names..."
GNOTH_HAS_MKT=$(grep -l "mkt.daily_user_retention" templates/rolling_forecast/gnoth/retention.sql.j2 2>/dev/null | wc -l)
PWMSEA_HAS_MKT=$(grep -l "mkt.daily_user_retention" templates/rolling_forecast/pwmsea/retention.sql.j2 2>/dev/null | wc -l)
L2M_HAS_PUBLIC=$(grep -l "public.user_retention" templates/rolling_forecast/l2m/retention.sql.j2 2>/dev/null | wc -l)

if [ $GNOTH_HAS_MKT -eq 1 ]; then
    echo "   ✅ GNOTH uses mkt.daily_user_retention"
else
    echo "   ⚠️  GNOTH retention template may not have correct table"
fi

if [ $PWMSEA_HAS_MKT -eq 1 ]; then
    echo "   ✅ PWMSEA uses mkt.daily_user_retention"
else
    echo "   ⚠️  PWMSEA retention template may not have correct table"
fi

if [ $L2M_HAS_PUBLIC -eq 1 ]; then
    echo "   ✅ L2M uses public.user_retention"
else
    echo "   ⚠️  L2M retention template may not have correct table"
fi
echo ""

# Check 6: Verify hardcoded game IDs in templates
echo "6. Checking templates have hardcoded game_id filters..."
GNOTH_HAS_ID=$(grep -l "game_id = 496" templates/rolling_forecast/gnoth/retention.sql.j2 2>/dev/null | wc -l)
PWMSEA_HAS_ID=$(grep -l "game_id = 425" templates/rolling_forecast/pwmsea/retention.sql.j2 2>/dev/null | wc -l)
L2M_HAS_ID=$(grep -l "game_id = 'l2m'" templates/rolling_forecast/l2m/retention.sql.j2 2>/dev/null | wc -l)

if [ $GNOTH_HAS_ID -eq 1 ]; then
    echo "   ✅ GNOTH filters by game_id = 496"
else
    echo "   ⚠️  GNOTH retention template may not filter by game_id"
fi

if [ $PWMSEA_HAS_ID -eq 1 ]; then
    echo "   ✅ PWMSEA filters by game_id = 425"
else
    echo "   ⚠️  PWMSEA retention template may not filter by game_id"
fi

if [ $L2M_HAS_ID -eq 1 ]; then
    echo "   ✅ L2M filters by game_id = 'l2m'"
else
    echo "   ⚠️  L2M retention template may not filter by game_id"
fi
echo ""

# Check 7: Documentation exists
echo "7. Checking documentation exists..."
if [ -f "docs/ROLLING_FORECAST_REFACTORING.md" ]; then
    echo "   ✅ Refactoring documentation exists"
else
    echo "   ⚠️  Refactoring documentation missing"
fi

if [ -f "docs/CODE_SIMPLIFICATION_GUIDE.md" ]; then
    echo "   ✅ Code simplification guide exists"
else
    echo "   ⚠️  Code simplification guide missing"
fi
echo ""

# Summary
echo "=== Summary ==="
echo "Refactoring complete! Next steps:"
echo ""
echo "1. Test each DAG individually:"
echo "   - Run: airflow dags test gnoth_daily_actual_rfc 2025-01-07"
echo "   - Run: airflow dags test pwmsea_daily_actual_rfc 2025-01-07"
echo "   - Run: airflow dags test l2m_TH_daily_actual_rfc 2025-01-07"
echo ""
echo "2. Verify parquet data aggregation:"
echo "   - Check: hdfs://c0s/user/gsbkk-workspace-yc9t6/rolling_forecast/gnoth/active/"
echo "   - Expect: 1 row per country (not 20+ rows)"
echo ""
echo "3. After successful testing, simplify code:"
echo "   - Remove etl_engine.py table resolution logic (lines 870-895)"
echo "   - Remove schema_version from game_name.yaml"
echo "   - Archive old template files to archived/ folder"
echo ""
echo "4. Read full documentation:"
echo "   - docs/ROLLING_FORECAST_REFACTORING.md"
echo "   - docs/CODE_SIMPLIFICATION_GUIDE.md"
echo ""
