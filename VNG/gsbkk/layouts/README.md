# Layouts Directory - JSON Configuration Files 📁

> **Simple Version:** Layout files tell the pipeline what to read, how to transform, and where to write data. Like recipes for data processing.

---

## What is a Layout File? (In Plain English)

Think of a layout file as a **recipe card**:

```json
{
  "description": "Extract active users from game database",
  
  "inputType": "jdbc",              ← WHERE to read FROM
  "inputConnection": "GDS_TRINO",   ← Which database
  "inputPath": "SELECT * FROM ...", ← What data to get
  
  "outputType": "file",             ← WHERE to write TO
  "outputPath": "hdfs://.../",      ← Where on HDFS
  "outputFormat": "parquet",        ← What format
  
  "schemas": [...]                  ← HOW to transform
}
```

**The pipeline reads this and executes the recipe automatically!**

---

## Directory Organization (4 Projects)

```
layouts/
├── game_health_check/        # Daily game metrics (DAU, revenue, VIP)
│   ├── etl/                  Extract from game databases
│   ├── cons/                 Consolidate + calculate VIP levels
│   └── sql/                  SQL templates (VIP, currency)
│
├── rolling_forecast/         # Daily revenue predictions
│   ├── etl/                  Read from TSN staging (REUSE health check!)
│   ├── cons/                 Market-specific aggregations
│   └── sql/                  SQL per market type (single/SEA/multi)
│
├── re-standardization/       # Data migration (old → new format)
│   ├── etl/                  Extract from legacy game DBs
│   ├── std/                  Transform to standard schema
│   ├── cons/                 Consolidated user profiles
│   └── sql/                  Complex joins + aggregations
│
└── sensortower/              # Market research (competitor data)
    ├── raw/                  API data definitions
    ├── etl/                  JSON → Parquet conversion
    └── std/                  Standardized tables
```

```json
{
  "description": "ETL example with schemas",
  
  "inputType": "jdbc",
  "inputConnection": "GDS_TRINO",
  "inputPath": "(SELECT user_id, amount FROM table WHERE ds = '{logDate}') as data",
  
  "outputType": "file",
  "outputPath": "hdfs://c0s/path/{logDate}",
  "outputFormat": "parquet",
  
  "schemas": [{
    "comment": "Transform columns",
    "columns": [
      {"colName": "user_id"},
      {"colName": "revenue", "source": "amount", "colType": "double"},
      {"colName": "date", "source": "'{logDate}'", "colType": "date"}
    ],
    "filters": ["revenue > 0"],
    "groupBy": ["user_id"],
    "orderBy": ["revenue DESC"]
  }]
}
```

### ✅ Complex Joins (Use SQL files)

For multi-source joins and complex logic:

```json
{
  "description": "Multi-source join example",
  
  "inputSources": [
    {
      "name": "daily_data",
      "type": "file",
      "path": "hdfs://c0s/etl/daily/{logDate}",
      "format": "parquet"
    },
    {
      "name": "prev_state",
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "query": "SELECT user_id, total FROM table WHERE ds < '{logDate}'"
    }
  ],
  
  "sqlFile": "sql/calculate_cumulative.sql",
  
  "output": {
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.result_table",
    "mode": "overwrite",
    "deleteCondition": "ds = '{logDate}'"
  }
}
```

### Dynamic Database & Safe Updates

- **Dynamic database**: Use `{gameId}` variable with `TSN_POSTGRES` connection
- **Safe incremental updates**: Use `deleteCondition` instead of full table overwrite

```json
{
  "output": {
    "type": "jdbc",
    "connection": "TSN_POSTGRES",  // Resolves to jdbc:postgresql://host:port/{gameId}
    "table": "public.metrics",
    "mode": "overwrite",
    "deleteCondition": "ds = '{logDate}'"  // Only deletes rows for this date
  }
}
```

## Processing Pipeline Stages

Each project follows a consistent 4-stage pipeline:

### 1. **RAW** - Source Data Acquisition
- **Purpose**: Acquire data from external sources
- **Methods**: 
  - API extraction (SensorTower, Facebook, TikTok)
  - Google Sheets read operations
  - Direct database queries
- **Output**: Raw JSON or CSV files in HDFS
- **Scripts**: 
  - `run_api_extraction.sh` for APIs
  - `run_gsheet_process.sh read` for Google Sheets

### 2. **ETL** - Extract, Transform, Load
- **Purpose**: Convert raw data into columnar format
- **Input**: Raw JSON/CSV from RAW stage
- **Output**: Parquet files in HDFS
- **Transformations**:
  - Data type casting
  - Basic filtering and validation
  - Column renaming
- **Script**: `run_etl_process.sh layouts/<project>/etl/<file>.json`

### 3. **STD** - Standardization
- **Purpose**: Apply business logic and standardize schemas
- **Input**: Parquet files from ETL stage
- **Output**: Standardized parquet files or Postgres tables
- **Transformations**:
  - Calculated fields (ARPU, retention rates, etc.)
  - Data normalization
  - Schema alignment across games
- **Script**: `run_etl_process.sh layouts/<project>/std/<file>.json`

### 4. **CONS** - Consolidation
- **Purpose**: Final aggregations and multi-source joins
- **Input**: Standardized data from STD stage
- **Output**: 
  - Postgres tables for dashboards
  - Google Sheets for stakeholders
  - Aggregated HDFS tables
- **Transformations**:
  - Multi-table joins
  - Time-series aggregations
  - Business intelligence metrics
- **Script**: `run_etl_process.sh layouts/<project>/cons/<file>.json`

## Project Details

### SensorTower (Market Research)
**Purpose**: Track mobile gaming market trends  
**Data Source**: SensorTower API  
**Pipeline**:
1. **RAW**: `run_api_extraction.sh sensortower <endpoint> <config> <date>`
2. **ETL**: Convert JSON to Parquet (top_games, new_games, metadata)
3. **STD**: Standardize game metadata and performance metrics
4. **CONS**: Market insights dashboard

**Example Workflow**:
```bash
# Step 1: Extract from SensorTower API
./run_api_extraction.sh sensortower top_games configs/api/sensortower.json 2024-12

# Step 2-4: Process through ETL → STD → CONS
./run_etl_process.sh "layouts/sensortower/etl/top_games.json,layouts/sensortower/std/top_games.json,layouts/sensortower/cons/market_insights.json" 2024-12
```

### Game Health Check
**Purpose**: Monitor daily game KPIs  
**Data Source**: Game databases (Trino/Iceberg)  
**Pipeline**:
1. **ETL**: Extract DAU, revenue, retention from game events
2. **STD**: Calculate ARPU, ARPPU, retention rates
3. **CONS**: Consolidated health metrics → Postgres

**Example Workflow**:
```bash
./run_etl_process.sh "layouts/game_health_check/etl/daily_kpi.json,layouts/game_health_check/std/daily_kpi.json,layouts/game_health_check/cons/health_metrics.json" 2024-12-25
```

### Rolling Forecast
**Purpose**: Weekly/monthly revenue and DAU forecasting  
**Data Source**: Google Sheets (input) + Historical data  
**Pipeline**:
1. **RAW**: `run_gsheet_process.sh read` - Read input from Sheets
2. **ETL**: Calculate forecasts with trend analysis
3. **STD**: Aggregate by week/month
4. **CONS**: `run_gsheet_process.sh write` - Write results to Sheets

**Example Workflow**:
```bash
# Step 1: Read input data from Google Sheets
./run_gsheet_process.sh read configs/gsheet/rfc_daily.json 2024-12-25

# Step 2-3: Process calculations
./run_etl_process.sh "layouts/rolling_forecast/etl/calculations.json,layouts/rolling_forecast/std/aggregated.json" 2024-12-25

# Step 4: Write results back to Sheets
./run_gsheet_process.sh write configs/gsheet/rfc_output.json 2024-12-25
```

### Re-standardization
**Purpose**: Migrate legacy game data to new standardized schema  
**Data Source**: Legacy game databases (L2M, etc.)  
**Pipeline**:
1. **ETL**: Extract from old schema (recharge, active users, etc.)
2. **STD**: Transform to standardized schema
3. **CONS**: Calculate derived metrics (cumulative revenue, user groups)

**Example Workflow (L2M)**:
```bash
# Process recharge data
./run_etl_process.sh "layouts/re-standardization/etl/l2m_recharge.json,layouts/re-standardization/std/l2m_recharge.json" 2024-12-25

# Process active users
./run_etl_process.sh "layouts/re-standardization/etl/l2m_active.json,layouts/re-standardization/std/l2m_active.json" 2024-12-25

# Calculate cumulative revenue
./run_etl_process.sh layouts/re-standardization/cons/l2m_cumulative_revenue.json 2024-12-25
```

## Layout File Format

Each layout JSON file defines:

```json
{
  "name": "layout_name",
  "description": "What this layout does",
  "source": {
    "type": "hadoop|trino|postgres|gsheet",
    "query": "SQL query or path"
  },
  "target": {
    "type": "hadoop|postgres|gsheet",
    "path": "output path",
    "table": "table name (for postgres)",
    "mode": "overwrite|append|upsert"
  },
  "transformations": [
    {"type": "filter", "condition": "..."},
    {"type": "add_column", "column": "...", "expression": "..."},
    {"type": "aggregate", "group_by": [...], "aggregations": {...}}
  ]
}
```

## Usage Patterns

### Sequential Processing (Recommended)
Process all stages in order for a single date:
```bash
./run_etl_process.sh "layouts/<project>/etl/<file>.json,layouts/<project>/std/<file>.json,layouts/<project>/cons/<file>.json" 2024-12-25
```

### Stage-by-Stage Processing
Process each stage separately (useful for debugging):
```bash
# ETL stage
./run_etl_process.sh layouts/<project>/etl/<file>.json 2024-12-25

# STD stage (after ETL completes)
./run_etl_process.sh layouts/<project>/std/<file>.json 2024-12-25

# CONS stage (after STD completes)
./run_etl_process.sh layouts/<project>/cons/<file>.json 2024-12-25
```

### Backfill Historical Data
Process multiple dates:
```bash
for date in 2024-12-{01..31}; do
    ./run_etl_process.sh "layouts/<project>/etl/<file>.json,layouts/<project>/std/<file>.json" $date
done
```

## Adding New Projects

To add a new project:

1. **Create directory structure**:
   ```bash
   mkdir -p layouts/<project_name>/{raw,etl,std,cons}
   ```

2. **Define layouts** for each stage:
   - `raw/<name>.json` - Document source data schema
   - `etl/<name>.json` - Raw → Parquet conversion
   - `std/<name>.json` - Standardization logic
   - `cons/<name>.json` - Final consolidation

3. **Update documentation**:
   - Add project description to this README
   - Include example workflow
   - Document any special requirements

4. **Test pipeline**:
   ```bash
   ./run_etl_process.sh layouts/<project_name>/etl/<file>.json <test_date>
   ```

## Best Practices

1. **One layout per logical transformation** - Don't combine too many transformations in one layout
2. **Idempotent processing** - Use `mode: "overwrite"` or `upsert_keys` for re-runnable jobs
3. **Partition by date** - Always partition HDFS output by date for efficient queries
4. **Version control layouts** - Treat layouts like code, commit to Git
5. **Document transformations** - Add clear descriptions explaining business logic
6. **Test with small date ranges** - Validate logic before backfilling historical data

## Troubleshooting

### Layout not found
```bash
# Ensure path is relative to gsbkk/ root or absolute
./run_etl_process.sh layouts/sensortower/etl/top_games.json 2024-12
```

### Source data missing
```bash
# Check RAW stage completed successfully
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/<project>/raw/<endpoint>/<date>/
```

### Transformation failed
```bash
# Run with single layout to isolate issue
./run_etl_process.sh layouts/<project>/etl/<file>.json 2024-12-25

# Check Spark logs for detailed error
cat /opt/airflow/logs/scheduler/<dag_id>/<task_id>/<date>/*.log
```

## Related Documentation

- [README.md](../README.md) - Project overview and architecture
- [QUICKSTART.md](../QUICKSTART.md) - Quick start examples
- [run_etl_process.sh](../run_etl_process.sh) - ETL processing script
- [run_api_extraction.sh](../run_api_extraction.sh) - API extraction script
- [run_gsheet_process.sh](../run_gsheet_process.sh) - Google Sheets integration
