# Rolling Forecast

Automated revenue forecasting and business planning data pipelines for game analytics.

## Overview

The Rolling Forecast project provides two complementary data flows:

1. **Traditional RFC** - Generate forecasts from game health data → push to Google Sheets for business review
2. **Google Sheets Import** ⭐ **NEW** - Import consolidated forecast data from Google Sheets → PostgreSQL for analysis

## Projects

### 1. Google Sheets Import (NEW)

**Purpose**: Import rolling forecast data from Google Sheets spreadsheet to PostgreSQL analytics database.

**Source**: [Rolling Forecast 2026 Spreadsheet](https://docs.google.com/spreadsheets/d/1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w)

**Data Flow**:
```
Google Sheets → ETL Engine → Spark SQL → PostgreSQL
  Daily Overall                          rfc_daily
  Monthly Overall                        rfc_monthly
```

**DAG**: [`gsheet_rfc_import_dag.py`](../../dags/rolling_forecast/gsheet_rfc_import_dag.py)
- **Schedule**: Daily at 9 AM
- **Tasks**: Import daily & monthly data (parallel execution)

**Layouts**:
- [`gsheet_daily_rfc.json`](gsheet_daily_rfc.json) - Daily metrics
- [`gsheet_monthly_rfc.json`](gsheet_monthly_rfc.json) - Monthly KPIs

**SQL Templates**:
- [`gsheet_daily_rfc.sql.j2`](../../templates/rolling_forecast/gsheet_daily_rfc.sql.j2)
- [`gsheet_monthly_rfc.sql.j2`](../../templates/rolling_forecast/gsheet_monthly_rfc.sql.j2)

**Output Tables**:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `rfc_daily` | Daily actuals vs forecast | game, date, install, nru, dau, pu, rev, rfc_* |
| `rfc_monthly` | Monthly KPIs vs actuals | game, month, product_group, market, *_kpi, *_rfc, *_actual |

**Key Features**:
- ✅ Type casting (Google Sheets text → date/numeric types)
- ✅ Year-based deletion (refreshes 2026 data only, preserves history)
- ✅ NULL row filtering (prevents empty row insertion)
- ✅ Idempotent updates (can re-run same date safely)

### 2. Traditional Rolling Forecast

**Purpose**: Generate revenue forecasts using game health metrics.

**Data Flow**:
```
PostgreSQL game_health → SQL aggregation → Google Sheets
  ghc_diagnostic_daily                     RFC Reports
```

**DAGs**:
- [`gnoth_rolling_forecast_dag.py`](../../dags/rolling_forecast/gnoth_rolling_forecast_dag.py)
- [`pwmsea_rolling_forecast_dag.py`](../../dags/rolling_forecast/pwmsea_rolling_forecast_dag.py)
- [`l2m_rolling_forecast_dag.py`](../../dags/rolling_forecast/l2m_rolling_forecast_dag.py)

**Layouts**: `cons/` subdirectories per game
- Example: [`gnoth/cons/rfc_metrics.json`](gnoth/cons/rfc_metrics.json)

**Market Types**:
| Type | Games | Output |
|------|-------|--------|
| Single | gnoth, slth | Thailand market only |
| SEA | pwmsea, omgthai | SEA aggregate |
| Multi | fw2, l2m | Multi-country breakdown |

## Usage

### Google Sheets Import

**Manual Run**:
```bash
# Import daily RFC
./run_etl_process.sh \
    --layout layouts/rolling_forecast/gsheet_daily_rfc.json \
    --gameId rfc \
    --logDate 2026-01-20

# Import monthly RFC
./run_etl_process.sh \
    --layout layouts/rolling_forecast/gsheet_monthly_rfc.json \
    --gameId rfc \
    --logDate 2026-01-20
```

**Airflow**: The DAG runs automatically daily at 9 AM, or trigger manually in Airflow UI.

### Traditional RFC

**Manual Run**:
```bash
./run_rfc_pipeline.sh <game_id> <date> <market_type>

# Examples:
./run_rfc_pipeline.sh gnoth 2026-01-20 single
./run_rfc_pipeline.sh pwmsea 2026-01-20 sea
./run_rfc_pipeline.sh l2m 2026-01-20 multi
```

## Configuration

### Google Sheets Import

**Daily Layout Structure**:
```json
{
  "inputSources": [{
    "name": "daily_rfc",
    "type": "gsheet",
   "sheet_id": "1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w",
    "range": "Daily Overall!A2:V"
  }],
  "sqlTemplate": "templates/rolling_forecast/gsheet_daily_rfc.sql.j2",
  "outputs": [{
    "type": "jdbc",
    "table": "public.rfc_daily",
    "mode": "append",
    "deleteCondition": "(date IS NULL OR game IS NULL) OR EXTRACT(YEAR FROM date) = 2026"
  }]
}
```

**Delete Condition Logic**:
```sql
-- Deletes:
-- 1. Any NULL rows (data quality cleanup)
-- 2. All 2026 data (full year refresh)
-- Preserves: 2025 and earlier data
(date IS NULL OR game IS NULL) OR EXTRACT(YEAR FROM date) = 2026
```

**Type Casting** (required for Google Sheets data):
```sql
-- Date columns
CAST(`Date` AS DATE)

-- Numeric columns
CAST(`Install` AS DOUBLE)
CAST(`Rev` AS DOUBLE)

-- Text columns (no cast needed)
`Game` as game
```

### Traditional RFC

**CONS Layout Structure**:
```json
{
  "inputSources": [{
    "name": "diagnostic_data",
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "sqlTemplate": "templates/rolling_forecast/gnoth/query.sql.j2"
  }],
  "sqlTemplate": "templates/rolling_forecast/gnoth/cons/metrics.sql.j2",
  "outputs": [{
    "type": "gsheet",
    "sheet_id": "...",
    "range": "Data!A:Z"
  }]
}
```

## Output Schema

### rfc_daily Table

| Column | Type | Description |
|--------|------|-------------|
| game | text | Game identifier |
| date | date | Report date |
| install | float8 | Daily installs |
| nru | float8 | New registered users |
| rr1 | float8 | Day 1 retention rate |
| dau | float8 | Daily active users |
| pu | float8 | Paying users |
| npu | float8 | New paying users |
| rev | float8 | Daily revenue |
| pr | float8 | Payment rate |
| arppu | float8 | Average revenue per paying user |
| cost | float8 | Marketing cost |
| rfc_install | float8 | Forecast install |
| rfc_nru | float8 | Forecast NRU |
| rfc_rr1 | float8 | Forecast retention |
| rfc_dau | float8 | Forecast DAU |
| rfc_pu | float8 | Forecast paying users |
| rfc_npu | float8 | Forecast new payers |
| rfc_rev | float8 | Forecast revenue |
| rfc_pr | float8 | Forecast payment rate |
| rfc_arppu | float8 | Forecast ARPPU |
| month | date | Month reference |
| updated_at | timestamp | Last update time |

### rfc_monthly Table

| Column | Type | Description |
|--------|------|-------------|
| game | text | Game identifier |
| product_group | text | Product category |
| market | text | Geographic market |
| team | text | Responsible team |
| month | text | Month (YYYY-MM format) |
| genre | text | Game genre |
| gross_rev_annual_kpi | float8 | Annual revenue KPI |
| profit_annual_kpi | float8 | Annual profit KPI |
| mau_annual_kpi | float8 | Annual MAU KPI |
| gross_rev_rfc | float8 | Rolling forecast revenue |
| profit_rfc | float8 | Rolling forecast profit |
| mau_rfc | float8 | Rolling forecast MAU |
| gross_rev_actual | float8 | Actual revenue |
| profit_actual | float8 | Actual profit |
| mau_actual | float8 | Actual MAU |
| ... | float8 | +24 more metric columns |
| updated_at | timestamp | Last update time |

## Troubleshooting

### NULL Rows in Database

**Symptom**: Table contains rows where all columns are NULL except `updated_at`.

**Cause**: Google Sheets range includes empty rows beyond actual data.

**Solution**: 
1. SQL template filters: `WHERE game IS NOT NULL AND game != ''`
2. DeleteCondition cleanup: `date IS NULL OR game IS NULL`
3. Limit range if needed: `A2:V5000` instead of `A2:V`

### Type Mismatch Errors

**Error**: `column "date" is of type date but expression is of type text`

**Cause**: Missing type casting in SQL template.

**Solution**: Add CAST operations:
```sql
CAST(`Date` AS DATE) as date
CAST(`Rev` AS DOUBLE) as rev
```

### Column Not Found Errors

**Error**: Column `` `Rev` `` not found

**Cause**: Column name mismatch (spaces, case sensitivity).

**Solution**: Match exact Google Sheets column names including spaces:
```sql
CAST(`Rev` AS DOUBLE)       -- if sheet has "Rev"
CAST(` Rev ` AS DOUBLE)     -- if sheet has " Rev " (with spaces)
```

### Duplicate Rows After Re-run

**Cause**: DeleteCondition not working properly.

**Check**:
1. Verify deleteCondition in layout file
2. Check PostgreSQL logs for DELETE query execution
3. Confirm WHERE clause references actual columns

### Google Sheets Credentials Error

**Error**: `Google Sheets credentials not found`

**Solution**: Verify `tsn-data-0e06f020fc9b.json` exists on HDFS and is copied to `gsheet_creds.json` in temp directory.

## Related Documentation

- [Google Sheets Integration Guide](../../docs/GOOGLE_SHEETS_INTEGRATION.md) - Technical details
- [ETL Engine Guide](../../docs/ETL_ENGINE_GUIDE.md) - Layout file format
- [DAG Development Guide](../../docs/DAG_DEVELOPMENT_GUIDE.md) - Creating DAGs

## Change Log

**2026-01-20**: Added Google Sheets import flow (gsheet_rfc_import_dag)
- New layouts for daily/monthly import
- PostgreSQL output tables (rfc_daily, rfc_monthly)
- Year-based delete condition for 2026 data refresh
