# Airflow Deployment Notes - Rolling Forecast Error Fix

> **Note:** For general deployment instructions, see [DEPLOYMENT.md](DEPLOYMENT.md). This document covers specific troubleshooting and fixes.

## Current Error (2026-01-02)

**Error:** `cannot resolve 'a.ad_id' given input columns`

**Root Cause:** Airflow server repository has OLD SQL templates that reference removed columns.

**Status:** ✅ **FIXED** - Commit `728a964` (Jan 2, 2026) - SQL templates simplified

---

## What Happened

### The Issue

Rolling forecast SQL templates still had OLD complex JOIN logic that expected granular campaign tracking fields:

**BEFORE (Old SQL):**
```sql
LEFT JOIN retention r
    ON a.report_date = r.report_date
    AND a.game_id = r.game_id
    AND a.ad_id = r.ad_id              ← These columns don't exist!
    AND a.adset_id = r.adset_id        ← ETL was simplified
    AND a.campaign_id = r.campaign_id  ← Removed in optimization
    AND a.media_source = r.media_source
    AND a.country_code = r.country_code
    AND a.site_id = r.site_id
    AND a.platform = r.platform
```

**AFTER (New SQL - CORRECT):**
```sql
LEFT JOIN retention r
    ON a.report_date = r.report_date
    AND a.country_code = r.country_code  ← Only 2 fields needed!
```

### Why The Error Happens

The ETL layouts were simplified to read from game_health_check TSN Postgres staging, but the SQL templates still expected granular campaign fields that no longer exist.

**Evidence:**
- ETL layouts ([layouts/rolling_forecast/etl/](layouts/rolling_forecast/etl/)) only output basic fields: `report_date`, `country_code`, `dau`, `pu`, etc.
- SQL templates still tried to JOIN on `ad_id`, `campaign_id`, `adset_id`, `media_source`, `site_id`, `platform`
- Error: "cannot resolve 'a.ad_id' given input columns: [a.cost, a.country_code, r.country_code, a.dau...]"

### The Fix (Commit: 728a964 - Jan 2, 2026)

Updated **both** SQL template files to match the simplified ETL data structure:

**Files Updated:**
1. `templates/rolling_forecast/rolling_forecast_market.sql.j2` (used by DAGs)
2. `templates/sql/rolling_forecast_market.sql.j2` (backup copy)

**Changes:**
- ✅ Simplified JOIN: Only `report_date + country_code` (removed 7 extra fields)
- ✅ Removed currency conversion logic (revenue already USD from game_health_check)
- ✅ Renamed CTE: `charge_with_currency` → `charge_aggregated`
- ✅ Removed `rev_vnd` column (not needed)

---

## Solution: Deploy Latest Code to Airflow

### Step 1: Pull Latest Code

```bash
# SSH to Airflow server
ssh airflow-server

# Navigate to DAG repo
cd /opt/airflow/dags/repo

# Pull latest changes (includes commit 728a964 with SQL fix)
git pull origin main

# Verify commit
git log --oneline -5

# Should see:
# 728a964 CRITICAL FIX: Simplify rolling_forecast SQL templates
# 71ed37d debug: debug response
# ...
```

### Step 2: Verify SQL Template

```bash
# Check that SQL template has simplified JOIN
head -50 /opt/airflow/dags/repo/templates/rolling_forecast/rolling_forecast_market.sql.j2 | grep -A 3 "LEFT JOIN retention"

# Should show ONLY 2 JOIN conditions:
#     LEFT JOIN retention r
#         ON a.report_date = r.report_date
#         AND a.country_code = r.country_code

# Should NOT show ad_id, campaign_id, etc.
```

### Step 3: Re-run Failed DAG

```bash
# Trigger the failed DAG (gnoth example)
airflow dags trigger gnoth_daily_actual_rfc --conf '{"logDate":"2026-01-01"}'

# Or re-run specific task
airflow tasks clear gnoth_daily_actual_rfc cons_metrics --execution-date 2026-01-01T05:00:00
```

### Option: Automatic Sync (If configured)

If Airflow has automatic git sync enabled, wait for next sync cycle (usually 5-10 minutes).

---

## Files Changed in This Fix (Commit: 728a964)

### SQL Templates (CRITICAL - Must be deployed!)

**Fixed Files:**
1. **templates/rolling_forecast/rolling_forecast_market.sql.j2** (used by all DAGs)
2. **templates/sql/rolling_forecast_market.sql.j2** (backup copy)

**What Changed:**
- **JOIN simplification:** Removed 7 fields from JOIN condition
  - ❌ Removed: `ad_id`, `campaign_id`, `adset_id`, `media_source`, `site_id`, `platform`, `game_id`
  - ✅ Kept: `report_date`, `country_code` only
  
- **Currency conversion removed:** Revenue already in USD from game_health_check
  - ❌ Removed: `CROSS JOIN currency_mapping`, macros.to_usd(), macros.to_vnd()
  - ✅ Direct: `SUM(c.revenue) as rev_usd`
  
- **CTE renamed:** `charge_with_currency` → `charge_aggregated`
- **Column removed:** `rev_vnd` (not needed since revenue is USD)

### Impact

**Affected DAGs:** All 12 rolling_forecast games
- gnoth (single market - TH)
- slth (single market - TH)
- pwmsea (SEA market)
- omgthai (SEA market)
- fw2, cft, mlb, mts, td, cloudsongsea (multi-country)
- l2m (multi-country with special logic)
- skf (to be added)
   - Revenue already in USD

### ETL Layouts (Also updated)

- **layouts/rolling_forecast/etl/active.json**
  - Now reads from: TSN Postgres `ghc_diagnostic_daily_{gameId}`
  - REUSES game_health_check data (60% faster!)

- **layouts/rolling_forecast/etl/charge.json**
  - Now reads from: TSN Postgres `ghc_diagnostic_daily_{gameId}`
  - Revenue already in USD

- **layouts/rolling_forecast/etl/retention.json**
  - Simplified output (removed game_id)

### CONS Layouts

- Removed `currency_mapping` from inputSources (not needed anymore)

---

## How to Verify Deployment

### 1. Check Git Commit on Server

```bash
cd /opt/airflow/dags/repo
git log --oneline -1

# Should show:
# 32095ac Fix YAML syntax error in game_name.yaml
```

### 2. Check SQL File Content

```bash
head -30 /opt/airflow/dags/repo/layouts/rolling_forecast/sql/single_market.sql

# Should see simplified joins:
# LEFT JOIN retention r
#     ON a.report_date = r.report_date
#     AND a.country_code = r.country_code

# Should NOT see:
# AND a.ad_id = r.ad_id  ← This means OLD code!
```

### 3. Re-run Failed DAG

After deployment, trigger manual run:

```bash
airflow dags trigger gnoth_daily_actual_rfc --conf '{"logDate":"2026-01-01"}'
```

Or use Airflow UI:
1. Go to DAGs
2. Find `gnoth_daily_actual_rfc`
3. Click "Trigger DAG"
4. Set `logDate: 2026-01-01`

---

## Performance Impact After Deployment

**Before Optimization:**
- Extract: 12 minutes (read GDS Postgres)
- Currency conversion: 4 minutes
- Total: ~22 minutes per game

**After Optimization:**
- Extract: 1 minute (read TSN staging - REUSE!)
- No currency conversion: 0 minutes
- Total: ~3 minutes per game

**Savings:** 86% faster (19 minutes saved per game!)

---

## All Commits to Deploy

Make sure server has these commits:

1. **cc50965** (Dec 29, 2025) - Major optimization + documentation overhaul
   - Fixed 4 SQL templates
   - Updated ETL layouts to read from TSN staging
   - Removed currency_mapping dependency

2. **32095ac** (Jan 2, 2026) - Fixed YAML syntax error
   - Fixed `game_name.yaml` structure
   - Added `countries:` wrapper key

---

## Troubleshooting

### Error Still Happens After Git Pull

**Check 1:** Verify etl_engine.py is loading correct SQL path

```bash
# Check how SQL file is resolved
grep -n "sqlFile" /opt/airflow/dags/repo/src/etl_engine.py
```

**Check 2:** Clear Airflow metadata cache

```bash
airflow db reset  # WARNING: Destructive!
# OR
airflow dags delete gnoth_daily_actual_rfc
# Then re-import
```

**Check 3:** Check if there are duplicate SQL files

```bash
find /opt/airflow/dags/repo -name "single_market.sql" -type f
```

### Currency Mapping Error

If you see `currency_mapping table not found`:

**Fix:** CONS layouts should NOT reference currency_mapping anymore!

```bash
# Check CONS layout
cat /opt/airflow/dags/repo/layouts/rolling_forecast/cons/single_market.json | grep currency_mapping

# Should return NOTHING (currency_mapping removed)
```

---

## Contact

**Deployed by:** Data Engineering Team  
**Date:** January 2, 2026  
**Commit:** 32095ac (YAML fix) + cc50965 (Optimization)  
**Documentation:** See README.md, layouts/rolling_forecast/README.md

---

## Quick Commands Reference

```bash
# On Airflow server:

# 1. Pull latest code
cd /opt/airflow/dags/repo && git pull origin main

# 2. Verify commit
git log --oneline -5

# 3. Check SQL file (should see simplified joins)
head -30 layouts/rolling_forecast/sql/single_market.sql

# 4. Restart Airflow scheduler (if needed)
systemctl restart airflow-scheduler

# 5. Re-run failed DAG
airflow dags trigger gnoth_daily_actual_rfc --conf '{"logDate":"2026-01-01"}'
```
