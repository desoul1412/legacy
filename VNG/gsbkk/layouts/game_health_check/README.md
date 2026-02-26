# Game Health Check

Daily monitoring and KPI reporting for all VNG games.

## Overview

Game Health Check provides automated daily ETL pipelines that:
- Extract transaction-level data from game databases (GDS Postgres/Trino)
- Standardize metrics across all games
- Generate consolidated KPI reports in TSN Postgres

**Games Supported**: GNOTH, L2M, PWMSEA

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    ETL STAGE                                 │
│  GDS Postgres/Trino → Extract raw transaction data → HDFS   │
│  - active_details   - charge_details   - campaign           │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    STD STAGE                                 │
│  HDFS Parquet → Standardize & aggregate → HDFS Parquet      │
│  - Normalize columns   - VIP calculations   - Retention     │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    CONS STAGE                                │
│  HDFS Parquet → Consolidate → TSN Postgres                  │
│  - diagnostic_daily   - package_performance   - server_*    │
└─────────────────────────────────────────────────────────────┘
```

## DAGs

Each game has a dedicated DAG:
- [`gnoth_game_health_check_dag.py`](../../dags/game_health_check/gnoth_game_health_check_dag.py) - Daily at 3 AM
- [`l2m_game_health_check_dag.py`](../../dags/game_health_check/l2m_game_health_check_dag.py) - Daily at 6 AM
- [`pwmsea_game_health_check_dag.py`](../../dags/game_health_check/pwmsea_game_health_check_dag.py) - Daily at 7 AM

## Layouts by Game

### GNOTH (Gunny Origin TH)
**Market**: Single (Thailand)  
**Schema**: GDS Postgres `common_tables_2`

**Pipeline**:
- **ETL**: [`gnoth/etl/`](gnoth/etl/) - Extract active_details, charge_details, campaign
- **STD**: [`gnoth/std/`](gnoth/std/) - Standardize active, charge, retention
- **CONS**: [`gnoth/cons/`](gnoth/cons/) - diagnostic_daily, package_performance, server_performance

### L2M (Lineage 2 Mobile)
**Market**: Multi (TH, ID,MY, PH, SG, VN)  
**Schema**: Custom per market

**Pipeline**:
- **ETL**: [`l2m/etl/`](l2m/etl/)
- **STD**: [`l2m/std/`](l2m/std/)
- **CONS**: [`l2m/cons/`](l2m/cons/)

### PWMSEA (Perfect World Mobile SEA)
**Market**: SEA aggregate  
**Schema**: GDS Trino data lake

**Pipeline**:
- **ETL**: [`pwmsea/etl/`](pwmsea/etl/)
- **STD**: [`pwmsea/std/`](pwmsea/std/)
- **CONS**: [`pwmsea/cons/`](pwmsea/cons/)

## Output Tables

All outputs written to TSN Postgres (`public` schema):

### diagnostic_daily_{game}

Daily KPIs and metrics overview.

| Column | Type | Description |
|--------|------|-------------|
| game_id | text | Game identifier |
| log_date | date | Report date |
| dau | bigint | Daily active users |
| new_user | bigint | New registered users |
| revenue | numeric | Total daily revenue |
| paying_user | bigint | Number of paying users |
| arpu | numeric | Average revenue per user |
| arppu | numeric | Average revenue per paying user |
| vip_breakdown | jsonb | Revenue by VIP level |
| updated_at | timestamp | Last update time |

### package_performance_{game}

Item sales performance by VIP segment.

| Column | Type | Description |
|--------|------|-------------|
| game_id | text | Game identifier |
| log_date | date | Report date |
| package_id | text | Item/package ID |
| package_name | text | Item name |
| vip_level | int | VIP tier |
| purchase_count | bigint | Number of purchases |
| revenue | numeric | Revenue from this item |
| unique_users | bigint | Unique purchasers |
| updated_at | timestamp | Last update time |

### server_performance_{game}

Server-level metrics for games with multi-server architecture.

| Column | Type | Description |
|--------|------|-------------|
| game_id | text | Game identifier |
| log_date | date | Report date |
| server_id | text | Server identifier |
| server_name | text | Server display name |
| dau | bigint | Daily active на server |
| new_user | bigint | New users on server |
| revenue | numeric | Server revenue |
| paying_user | bigint | Payers on server |
| updated_at | timestamp | Last update time |

## Usage

### Run Full Pipeline

```bash
# Manual execution for specific game and date
./run_game_health_pipeline.sh <date> [game_id] [report_type]

# Examples:
./run_game_health_pipeline.sh 2026-01-20 gnoth
./run_game_health_pipeline.sh 2026-01-20 l2m
./run_game_health_pipeline.sh 2026-01-20 pwmsea daily
```

### Run Individual Stage

```bash
# ETL stage only
./run_etl_process.sh \
    --layout layouts/game_health_check/gnoth/etl/active_details.json \
    --gameId gnoth \
    --logDate 2026-01-20

# STD stage only
./run_etl_process.sh \
    --layout layouts/game_health_check/gnoth/std/active.json \
    --gameId gnoth \
    --logDate 2026-01-20

# CONS stage only
./run_etl_process.sh \
    --layout layouts/game_health_check/gnoth/cons/diagnostic_daily.json \
    --gameId gnoth \
    --logDate 2026-01-20
```

## Configuration

### ETL Stage Layouts

Extract raw data from source databases.

**Example**: [`gnoth/etl/active_details.json`](gnoth/etl/active_details.json)
```json
{
  "inputSources": [{
    "name": "active_data",
    "type": "jdbc",
    "connection": "GDS_POSTGRES",
    "tableKey": "daily_active",
    "sqlTemplate": "templates/game_health_check/gnoth/etl/active_details.sql.j2"
  }],
  "outputs": [{
    "type": "file",
    "path": "hdfs://c0s/user/gsbkk/game_health_check/gnoth/etl/active/{logDate}",
    "format": "parquet"
  }]
}
```

### STD Stage Layouts

Standardize and aggregate data.

**Example**: [`gnoth/std/active.json`](gnoth/std/active.json)
```json
{
  "inputSources": [{
    "name": "etl_active",
    "type": "file",
    "path": "hdfs://c0s/user/gsbkk/game_health_check/gnoth/etl/active/{logDate}",
    "format": "parquet"
  }],
  "sqlTemplate": "templates/game_health_check/gnoth/std/active.sql.j2",
  "outputs": [{
    "type": "file",
    "path": "hdfs://c0s/user/gsbkk/game_health_check/gnoth/std/active/{logDate}",
    "format": "parquet"
  }]
}
```

### CONS Stage Layouts

Consolidate to PostgreSQL.

**Example**: [`gnoth/cons/diagnostic_daily.json`](gnoth/cons/diagnostic_daily.json)
```json
{
  "inputSources": [
    {"name": "active", "type": "file", "path": "hdfs://.../std/active/{logDate}"},
    {"name": "charge", "type": "file", "path": "hdfs://.../std/charge/{logDate}"}
  ],
  "sqlTemplate": "templates/game_health_check/gnoth/cons/diagnostic_daily.sql.j2",
  "outputs": [{
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.ghc_diagnostic_daily_gnoth",
    "mode": "overwrite",
    "deleteCondition": "log_date = '{logDate}'"
  }]
}
```

## SQL Templates

Templates use Jinja2 for common calculations:

**VIP Level Calculation**:
```sql
{% import 'sql/macros.sql' as macros %}

SELECT 
    role_id,
    {{ macros.calculate_vip_level('total_revenue') }} as vip_level
FROM ...
```

**Currency Conversion**:
```sql
SELECT 
    revenue_local,
    {{ macros.convert_currency('revenue_local', 'exchange_rate') }} as revenue_usd
FROM ...
```

## Downstream Dependencies

Game Health Check data feeds:
1. **Rolling Forecast** - Uses `ghc_diagnostic_daily_*` for predictions
2. **Business Reports** - Google Sheets exports
3. **Analytics Dashboards** - Direct PostgreSQL queries

**DAG Triggers**:
```python
# In game_health_check DAG
trigger_rfc = TriggerDagRunOperator(
    task_id='trigger_rolling_forecast',
    trigger_dag_id='gnoth_daily_actual_rfc',
    ...
)
```

## Troubleshooting

### Missing Data in Output Tables

**Check**:
1. Verify source database has data for date
2. Check ETL stage output: `hdfs dfs -ls hdfs://c0s/.../etl/active/{date}`
3. Check Airflow logs for errors
4. Verify table schema matches template output

### VIP Calculations Incorrect

**Issue**: VIP levels not matching business logic

**Solution**: Update VIP thresholds in `templates/sql/macros.sql`:
```sql
{% macro calculate_vip_level(revenue_column) %}
CASE 
    WHEN {{ revenue_column }} >= 10000000 THEN 10
    WHEN {{ revenue_column }} >= 5000000 THEN 9
    ...
{% endmacro %}
```

### Performance Issues

**Symptoms**: DAG takes >2 hours to complete

**Optimize**:
1. Add partitioning in STD outputs: `"numPartitions": 8`
2. Use JDBC batch settings: `"options": {"batchsize": "10000"}`
3. Filter early in ETL SQL templates
4. Check Spark executor memory

## Adding New Games

1. **Create layout directories**:
```bash
mkdir -p layouts/game_health_check/{game_id}/{etl,std,cons}
```

2. **Copy and modify layouts** from similar game (gnoth for single market, l2m for multi-market)

3. **Create SQL templates** in `templates/game_health_check/{game_id}/`

4. **Create DAG** in `dags/game_health_check/{game_id}_game_health_check_dag.py`

5. **Configure game info** in `configs/game_configs/{game_id}/info.json`

6. **Test pipeline**:
```bash
./run_game_health_pipeline.sh 2026-01-20 {game_id}
```

## Related Documentation

- [ETL Engine Guide](../../docs/ETL_ENGINE_GUIDE.md) - Layout file format
- [SQL Template Guide](../../docs/WIKI_SQL_TEMPLATE_GUIDE.md) - Jinja2 patterns
- [DAG Development Guide](../../docs/DAG_DEVELOPMENT_GUIDE.md) - Creating DAGs
- [Rolling Forecast README](../rolling_forecast/README.md) - Downstream consumer

## Change Log

**2026-01-20**: Added PWMSEA schedule adjustment (6 AM → 7 AM)
