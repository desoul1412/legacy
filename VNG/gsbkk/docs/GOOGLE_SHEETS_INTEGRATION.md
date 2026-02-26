# Google Sheets Integration Guide

Technical guide for using Google Sheets as an input or output source in GSBKK ETL pipelines.

## Overview

The ETL engine supports Google Sheets integration for bi-directional data sync:
- **Read**: Import data from Google Sheets → transform → write to PostgreSQL/HDFS
- **Write**: Read from PostgreSQL/HDFS → transform → export to Google Sheets

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GOOGLE SHEETS                             │
│  Spreadsheet ID: 1ABC...xyz                                 │
│  └─ Sheet: "Daily Overall" (Range: A2:V)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    ETL ENGINE                                │
│  ┌───────────────┐  ┌───────────────┐  ┌──────────────────┐ │
│  │  gspread API  │  │  Convert to   │  │  Spark DataFrame │ │
│  │  (Read Sheet) │─▶│  Spark Schema │─▶│  (Transform)     │ │
│  └───────────────┘  └───────────────┘  └──────────────────┘ │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    OUTPUT                                    │
│  PostgreSQL / HDFS Parquet / Google Sheets                  │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

### 1. Google Service Account

**Credentials File**: `tsn-data-0e06f020fc9b.json`
- Stored on HDFS: `hdfs://nameservice/configs/tsn-data-0e06f020fc9b.json`
- Downloaded to temp directory during job execution
- Copied to `gsheet_creds.json` for compatibility

**Permissions Required**:
- Google Sheets API enabled
- Service account has editor/viewer access to target spreadsheets

### 2. Python Dependencies

```python
gspread>=5.0.0
oauth2client>=4.1.3
```

## Configuration

### Input Source (Read from Google Sheets)

**Layout Configuration**:
```json
{
  "inputSources": [
    {
      "name": "gsheet_data",
      "type": "gsheet",
      "sheet_id": "1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w",
      "range": "Sheet Name!A2:Z",
      "comment": "Range format: WorksheetName!CellRange"
    }
  ],
  "sqlTemplate": "templates/path/transform.sql.j2",
  "outputs": [...]
}
```

**Parameters**:
| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `name` | Yes | Temp view name in Spark SQL | `daily_rfc` |
| `type` | Yes | Must be `"gsheet"` | `gsheet` |
| `sheet_id` | Yes | Google Sheets spreadsheet ID | `1ABC...xyz` |
| `range` | Yes | Worksheet name + cell range | `Daily Overall!A2:V` |

**Range Format**:
```
WorksheetName!StartCell:EndCell

Examples:
"Sheet1!A1:Z"          - From A1 to last row in column Z
"Daily Overall!A2:V"   - Skip header, columns A through V
"Data!A2:V5000"        - Limit to 5000 rows (prevents reading empty rows)
```

### Output (Write to Google Sheets)

**Layout Configuration**:
```json
{
  "inputSources": [...],
  "sqlTemplate": "templates/path/transform.sql.j2",
  "outputs": [
    {
      "type": "gsheet",
      "sheet_id": "1ABC...xyz",
      "range": "Output!A:Z",
      "columns": ["date", "revenue", "dau"],
      "update_mode": "append"
    }
  ]
}
```

**Parameters**:
| Parameter | Required | Description |
|-----------|----------|-------------|
| `sheet_id` | Yes | Target spreadsheet ID |
| `range` | Yes | Worksheet + range to write |
| `columns` | No | Column order (default: all) |
| `update_mode` | No | `append` or `overwrite` (default) |

## Data Type Handling

### Google Sheets → Spark SQL

**Issue**: Google Sheets returns **all data as text**, including numbers and dates.

**Solution**: Explicit type casting in SQL templates.

```sql
-- ❌ WRONG: Will read as STRING
SELECT `Date` as date FROM gsheet_data

-- ✅ CORRECT: Cast to proper types
SELECT 
    CAST(`Date` AS DATE) as date,
    CAST(`Revenue` AS DOUBLE) as revenue,
    CAST(`Installs` AS INT) as installs,
    `Game` as game  -- Text columns don't need casting
FROM gsheet_data
```

**Supported Type Casts**:
```sql
CAST(column AS DATE)        -- For date columns
CAST(column AS TIMESTAMP)   -- For datetime columns
CAST(column AS DOUBLE)      -- For decimals (revenue, metrics)
CAST(column AS FLOAT)       -- Alternative for decimals
CAST(column AS INT)         -- For integers (counts, IDs)
CAST(column AS BIGINT)      -- For large integers
CAST(column AS STRING)      -- Explicit string (usually not needed)
```

### Spark SQL → Google Sheets

Spark data types are automatically converted to Google Sheets format:
- `DATE` → Text (YYYY-MM-DD)
- `DOUBLE` → Number
- `STRING` → Text
- `NULL` → Empty cell

## Common Patterns

### Pattern 1: Import Daily Metrics

**Use Case**: Import daily game metrics from business team's Google Sheet.

**Layout**:
```json
{
  "inputSources": [{
    "name": "daily_metrics",
    "type": "gsheet",
    "sheet_id": "1ABC...xyz",
    "range": "Daily!A2:Z"
  }],
  "sqlTemplate": "templates/transform.sql.j2",
  "outputs": [{
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.daily_metrics",
    "mode": "append",
    "deleteCondition": "date = DATE('{logDate}')"
  }]
}
```

**SQL Template**:
```sql
SELECT
    `Game` as game,
    CAST(`Date` AS DATE) as date,
    CAST(`DAU` AS DOUBLE) as dau,
    CAST(`Revenue` AS DOUBLE) as revenue
FROM daily_metrics
WHERE `Game` IS NOT NULL AND `Game` != ''
  AND `Date` IS NOT NULL AND `Date` != ''
```

### Pattern 2: Export to Google Sheets

**Use Case**: Push analytics results to Google Sheets for stakeholder review.

**Layout**:
```json
{
  "inputSources": [{
    "name": "analytics_data",
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "sqlTemplate": "templates/query.sql.j2"
  }],
  "sqlTemplate": "templates/format.sql.j2",
  "outputs": [{
    "type": "gsheet",
    "sheet_id": "1ABC...xyz",
    "range": "Results!A:Z",
    "update_mode": "overwrite"
  }]
}
```

### Pattern 3: NULL Row Prevention

**Problem**: Google Sheets range `A2:Z` reads ALL rows to end of sheet, including thousands of empty rows.

**Solution 1 - Filter in SQL**:
```sql
SELECT ...
FROM gsheet_data
WHERE `Game` IS NOT NULL AND `Game` != ''
  AND `Date` IS NOT NULL AND `Date` != ''
```

**Solution 2 - Limit Range**:
```json
{
  "range": "Daily!A2:V5000"  // Only read first 5000 rows
}
```

**Solution 3 - Delete Condition Cleanup**:
```json
{
  "outputs": [{
    "type": "jdbc",
    "deleteCondition": "(date IS NULL OR game IS NULL) OR date = DATE('{logDate}')"
  }]
}
```

### Pattern 4: Idempotent Updates

**Problem**: Need to re-run same date without creating duplicates.

**Solution**: Use `deleteCondition` before insert:
```json
{
  "outputs": [{
    "type": "jdbc",
    "table": "public.rfc_daily",
    "mode": "append",
    "deleteCondition": "game IN (SELECT DISTINCT game FROM temp_table) AND date = DATE('{logDate}')"
  }]
}
```

**How it works**:
1. ETL engine writes transformed data to temp table
2. Executes DELETE query with condition
3. Inserts data from temp table
4. Commits transaction (or rollback if error)

## Column Name Handling

### Case Sensitivity

Google Sheets column names are **case-sensitive** in Spark SQL.

```sql
-- If Google Sheet has "Revenue" (capital R)
SELECT `Revenue` AS revenue  -- ✅ WORKS
SELECT `revenue` AS revenue  -- ❌ FAILS

-- If Google Sheet has "revenue" (lowercase r)
SELECT `revenue` AS revenue  -- ✅ WORKS
SELECT `Revenue` AS revenue  -- ❌ FAILS
```

### Spaces in Column Names

**Always use backticks** for column names with spaces:

```sql
-- Google Sheet column: "RFC Install"
SELECT `RFC Install` as rfc_install  -- ✅ WORKS
SELECT RFC Install as rfc_install    -- ❌ SYNTAX ERROR
```

**Trailing spaces matter**:
```sql
-- If column is " Rev " (with spaces)
CAST(` Rev ` AS DOUBLE)  -- ✅ Exact match

-- If column is "Rev" (no spaces)
CAST(`Rev` AS DOUBLE)    -- ✅ Exact match
```

### Special Characters

**Use backticks** for columns with special characters:

```sql
SELECT 
    `Game ID` as game_id,
    `Revenue (USD)` as revenue_usd,
    `D1 Retention %` as d1_retention_pct
FROM gsheet_data
```

## Credentials Management

### Setup Flow

```bash
# 1. Credentials stored on HDFS
hdfs://nameservice/configs/tsn-data-0e06f020fc9b.json

# 2. run_etl_process.sh downloads to temp directory
TEMP_CREDS_DIR=/tmp/creds_$$
hdfs dfs -get ".../tsn-data-0e06f020fc9b.json" "$TEMP_CREDS_DIR/"

# 3. Copy to expected filename for ETL engine
cp "$TEMP_CREDS_DIR/tsn-data-0e06f020fc9b.json" \
   "$TEMP_CREDS_DIR/gsheet_creds.json"

# 4. ETL engine uses gsheet_creds.json
python src/etl_engine.py --layout ... --gsheet-creds "$TEMP_CREDS_DIR/gsheet_creds.json"
```

### Troubleshooting Credentials

**Error**: `Google Sheets credentials not found`

**Check**:
```bash
# 1. Verify file exists on HDFS
hdfs dfs -ls hdfs://nameservice/configs/tsn-data-0e06f020fc9b.json

# 2. Check permissions
hdfs dfs -ls -d hdfs://nameservice/configs/

# 3. Check download succeeded (in Airflow logs)
grep "Google Sheets credentials" <task_log>
```

**Error**: `insufficient authentication scopes`

**Solution**: Update service account permissions in Google Cloud Console.

## Performance Considerations

### Reading Large Sheets

**Issue**: Google Sheets API has rate limits and slow response for large datasets.

**Best Practices**:
1. **Limit range**: Use `A2:V5000` instead of `A2:V`
2. **Filter in SQL**: Remove unnecessary rows early
3. **Partition data**: Split by date/game if possible
4. **Cache results**: Write to HDFS parquet for downstream use

### Writing Large Datasets

**Limitation**: Google Sheets has 10M cell limit per spreadsheet.

**Workarounds**:
1. **Aggregate**: Only write summary data to sheets
2. **Split sheets**: Multiple output sheets for different dimensions
3. **Use PostgreSQL**: Store detailed data in database, sheets for summaries

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `WorksheetNotFound` | Worksheet name mismatch | Check exact name in `range` |
| `Column not found` | Column name/case mismatch | Match exact Google Sheet names |
| `Type mismatch` | Missing CAST | Add type casting in SQL |
| `NULL rows inserted` | Empty rows in sheet | Add WHERE filters |
| `API quota exceeded` | Too many requests | Add backoff/retry logic |

### Debugging Tips

**View Raw Data**:
```sql
-- See what Spark reads from Google Sheets
SELECT * FROM gsheet_data LIMIT 10
```

**Check Schema**:
```sql
-- All columns come in as STRING from Google Sheets
DESCRIBE gsheet_data
```

**Test Casting**:
```sql
-- Verify type casts work before full pipeline
SELECT 
    `Date`,
    CAST(`Date` AS DATE) as date_cast,
    TYPEOF(CAST(`Date` AS DATE)) as date_type
FROM gsheet_data
LIMIT 5
```

## Best Practices

1. **Always filter NULL rows**: `WHERE column IS NOT NULL AND column != ''`
2. **Always cast types**: Don't assume Google Sheets types
3. **Use backticks**: For all column names with spaces/special chars
4. **Limit ranges**: Avoid reading empty rows (A2:V5000 vs A2:V)
5. **Use delete conditions**: For idempotent updates
6. **Test with small data**: Verify types and formats before full run
7. **Document column names**: List exact Google Sheet column names in SQL template comments

## Examples

See working examples:
- [layouts/rolling_forecast/gsheet_daily_rfc.json](../layouts/rolling_forecast/gsheet_daily_rfc.json)
- [templates/rolling_forecast/gsheet_daily_rfc.sql.j2](../templates/rolling_forecast/gsheet_daily_rfc.sql.j2)

## Related Documentation

- [Rolling Forecast README](../layouts/rolling_forecast/README.md) - Google Sheets import example
- [ETL Engine Guide](ETL_ENGINE_GUIDE.md) - Layout file format
- [JINJA2_GUIDE.md](../JINJA2_GUIDE.md) - SQL template system
