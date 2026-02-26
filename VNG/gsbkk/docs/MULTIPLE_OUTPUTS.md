# Multiple Outputs Enhancement

> **Note:** This document describes a legacy enhancement. For current layout format documentation, see [WIKI_LAYOUT_GUIDE.md](WIKI_LAYOUT_GUIDE.md).

## Overview
Enhanced the ETL engine to support writing data to multiple destinations (Postgres, GSheet, HDFS) from a single layout file. This eliminates the need for separate output tasks in DAGs.

**Current Status:** The `outputs` array format is now the standard. See [WIKI_LAYOUT_GUIDE.md](WIKI_LAYOUT_GUIDE.md) for complete documentation.

## Architecture

### Before (Separate Tasks)
```
ETL Tasks → CONS Task (Postgres) → OUTPUT Task (GSheet)
```
- 2 separate tasks for CONS and OUTPUT
- Data read twice (from Postgres to GSheet)
- More DAG complexity

### After (Multiple Outputs)
```
ETL Tasks → CONS Task (Postgres + GSheet simultaneously)
```
- Single task with multiple outputs
- Data computed once, written to multiple destinations
- Atomic operation (both succeed or both fail)
- Simpler DAG structure

## Code Changes

### 1. ETL Engine (`src/etl_engine.py`)

**Enhanced `process_layout()` function:**
- Supports both `output` (single) and `outputs` (array) in layout JSON
- Backward compatible with existing layouts
- Loops through each output destination

**Added `write_output()` function:**
- Handles 'file', 'jdbc', and 'gsheet' output types
- For 'gsheet': Loads config and calls gsheet_processor

**Example layout structure:**
```json
{
  "outputs": [
    {
      "name": "postgres",
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "table": "public.rfc_daily_{gameId}",
      "mode": "overwrite",
      "deleteCondition": "report_date = '{logDate}'"
    },
    {
      "name": "gsheet",
      "type": "gsheet",
      "config_file": "configs/gsheet/rfc_daily_{gameId}.json"
    }
  ]
}
```

### 2. GSheet Processor (`src/gsheet_processor.py`)

**Enhanced `write_to_gsheet()` function:**
- Added `df_input` parameter to accept pre-loaded DataFrame
- Added `input_type='dataframe'` mode to bypass reading from source
- Made `date` parameter optional when DataFrame is provided

**Signature change:**
```python
# Before
def write_to_gsheet(config: dict, date: str, spark: SparkSession)

# After
def write_to_gsheet(spark: SparkSession, config: dict, date: str = None, df_input=None)
```

### 3. Layout Updates (`layouts/rolling_forecast/cons/metrics.json`)

**Changed from single output to multiple outputs:**
```json
// Before
"output": {
  "type": "jdbc",
  "connection": "TSN_POSTGRES",
  "table": "public.rfc_daily_{gameId}",
  "mode": "overwrite",
  "deleteCondition": "report_date = '{logDate}'"
}

// After
"outputs": [
  {
    "name": "postgres",
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.rfc_daily_{gameId}",
    "mode": "overwrite",
    "deleteCondition": "report_date = '{logDate}'"
  },
  {
    "name": "gsheet",
    "type": "gsheet",
    "config_file": "configs/gsheet/rfc_daily_{gameId}.json"
  }
]
```

### 4. DAG Simplification (`dags/rolling_forecast/consolidated_rolling_forecast_dag.py`)

**Removed separate GSheet task:**
```python
# Before
output_gsheet = BashOperator(
    task_id='output_gsheet',
    bash_command="cd /opt/airflow/dags/repo && ./run_gsheet_process.sh write configs/gsheet/rfc_daily_{{ params.game_id }}.json {{ ds }}",
    params={'game_id': game_id},
    dag=dag,
)

# Dependencies
etl_complete >> tasks['cons_metrics'] >> output_gsheet >> end

# After - No separate task needed
# Dependencies
etl_complete >> tasks['cons_metrics'] >> end
```

## Benefits

1. **Simpler DAG**: Fewer tasks to manage (5 → 4 tasks)
2. **Atomic Operations**: Both Postgres and GSheet writes succeed or fail together
3. **Better Performance**: DataFrame computed once and written to multiple destinations
4. **Cleaner Architecture**: All outputs defined in single layout file
5. **Reduced Complexity**: No need for separate config files and bash commands
6. **Backward Compatible**: Existing layouts with single `output` still work

## Usage

### Single Output (Backward Compatible)
```json
{
  "output": {
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.my_table"
  }
}
```

### Multiple Outputs (New Feature)
```json
{
  "outputs": [
    {
      "name": "postgres",
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "table": "public.my_table"
    },
    {
      "name": "hdfs_backup",
      "type": "file",
      "path": "hdfs://c0s/user/backup/my_table/{date}",
      "format": "parquet"
    },
    {
      "name": "gsheet_report",
      "type": "gsheet",
      "config_file": "configs/gsheet/my_report.json"
    }
  ]
}
```

## GSheet Output Configuration

The `gsheet` output type requires a config file with Google Sheets details:

**configs/gsheet/rfc_daily_gnoth.json:**
```json
{
  "sheet_id": "YOUR_SHEET_ID",
  "range": "Daily Actual!A3",
  "columns": ["report_date", "install", "nru", "rr01", "rr07", "dau", "pu", "npu", "rev_usd"],
  "sort_by": [{"column": "report_date", "order": "desc"}],
  "update_mode": "upsert",
  "date_column": "report_date",
  "key_column": "report_date"
}
```

## Testing

Test with a sample layout:
```bash
spark-submit src/etl_engine.py \
  --layout layouts/rolling_forecast/cons/metrics.json \
  --var gameId=gnoth \
  --var logDate=2025-01-04
```

Check that:
1. Data is written to `public.rfc_daily_gnoth` in Postgres
2. Data is written to Google Sheets specified in config
3. Both outputs have the same data (atomic operation)

## Migration Guide

To migrate existing DAGs to use multiple outputs:

1. **Update layout file**: Change `output` to `outputs` array
2. **Add gsheet output**: Include gsheet config in outputs array
3. **Remove separate task**: Delete BashOperator for GSheet writing
4. **Update dependencies**: Point directly from cons task to end
5. **Test**: Run DAG and verify both outputs

## Notes

- The `config_file` path in gsheet output supports variable substitution (e.g., `{gameId}`, `{logDate}`)
- GSheet configs must exist before running the DAG
- Update modes: 'append', 'overwrite', 'upsert' (find by date and update)
- For large datasets, consider performance implications of writing to Google Sheets
