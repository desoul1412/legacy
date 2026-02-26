# GSBKK Framework - Quick Start Guide

## Overview

Three core scripts for different data sources:
- **`run_etl_process.sh`** - Database ETL (Trino, Postgres, HDFS)
- **`run_api_extraction.sh`** - External APIs (SensorTower, Facebook, TikTok)
- **`run_gsheet_process.sh`** - Google Sheets I/O

## Quick Examples

### Re-standardization (L2M)

```bash
# ETL: Trino → Parquet
./run_etl_process.sh --layout layouts/re-standardization/etl/l2m_recharge.json --gameId l2m --logDate 2024-12-25

# STD: Parquet → Postgres (atomic delete+insert)
./run_etl_process.sh --layout layouts/re-standardization/std/l2m_recharge.json --gameId l2m --logDate 2024-12-25

# CONS: Multi-source join with SQL file
./run_etl_process.sh --layout layouts/re-standardization/cons/l2m_cumulative_revenue.json --gameId l2m --logDate 2024-12-25
```

### SensorTower Market Research

```bash
# RAW: Extract from API
./run_api_extraction.sh sensortower top_games configs/api/sensortower.json 2024-12

# ETL: JSON → Parquet
./run_etl_process.sh --layout layouts/sensortower/etl/top_games.json --month 2024-12

# STD: Parquet → Postgres
./run_etl_process.sh --layout layouts/sensortower/std/top_games.json --month 2024-12
```

### Game Health Check

```bash
# ETL → STD → CONS pipeline
./run_etl_process.sh --layout layouts/game_health_check/etl/daily_kpi.json --gameId l2m --logDate 2024-12-25
./run_etl_process.sh --layout layouts/game_health_check/std/daily_kpi.json --gameId l2m --logDate 2024-12-25
./run_etl_process.sh --layout layouts/game_health_check/cons/health_metrics.json --gameId l2m --logDate 2024-12-25
```

## Layout Formats

### Simple Transformations (use `schemas`)

```json
{
  "inputType": "jdbc",
  "inputConnection": "GDS_TRINO",
  "inputPath": "(SELECT * FROM table WHERE ds = '{logDate}') as data",
  
  "outputType": "file",
  "outputPath": "hdfs://c0s/path/{logDate}",
  "outputFormat": "parquet",
  
  "schemas": [{
    "columns": [
      {"colName": "user_id"},
      {"colName": "revenue", "source": "amount", "colType": "double"}
    ],
    "filters": ["revenue > 0"],
    "groupBy": ["user_id"]
  }]
}
```

### Complex Joins (use SQL files)

```json
{
  "inputSources": [
    {
      "name": "daily_data",
      "type": "file",
      "path": "hdfs://c0s/etl/{logDate}",
      "format": "parquet"
    },
    {
      "name": "prev_state",
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "query": "SELECT * FROM table WHERE ds < '{logDate}'"
    }
  ],
  
  "sqlFile": "sql/calculate_cumulative.sql",
  
  "output": {
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.result",
    "mode": "overwrite",
    "deleteCondition": "ds = '{logDate}'"
  }
}
```

## Key Features

### Dynamic Database
```json
{"connection": "TSN_POSTGRES"}  // → jdbc:postgresql://host:port/{gameId}
```

### Safe Incremental Updates
```json
{
  "mode": "overwrite",
  "deleteCondition": "ds = '{logDate}'"  // Only deletes this date
}
```

### Variable Substitution
- `{gameId}` → Game identifier (l2m, cft, cs)
- `{logDate}` → Processing date (YYYY-MM-DD)
- `{month}` → Year-month (YYYY-MM)

## Airflow Integration

```python
from airflow.operators.bash_operator import BashOperator

task = BashOperator(
    task_id='etl_recharge',
    bash_command="""
        cd /opt/airflow/dags/repo
        ./run_etl_process.sh \
            --layout layouts/re-standardization/etl/l2m_recharge.json \
            --gameId l2m \
            --logDate {{ ds }}
    """,
    dag=dag,
)
```

## Documentation

- **DAGs**: [dags/README.md](dags/README.md) - DAG organization and examples
- **Layouts**: [layouts/README.md](layouts/README.md) - Layout file structure
- **Main Documentation**: [README.md](README.md) - Project overview and architecture
- **Jinja2 Templates**: [JINJA2_GUIDE.md](JINJA2_GUIDE.md) - Template system guide
