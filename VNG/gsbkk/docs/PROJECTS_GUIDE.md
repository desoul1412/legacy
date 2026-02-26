# GSBKK Projects - Complete Guide

This document provides an overview of all active projects in the GSBKK data pipeline framework, with links to detailed project-specific documentation.

---

## 📚 Projects Overview

| Project | Purpose | Status | Documentation |
|---------|---------|--------|---------------|
| **[Re-standardization](#1-re-standardization)** | Migrate legacy game data to standardized schema | ✅ Active (L2M) | [README](re-standardization/README.md) |
| **[SensorTower](#2-sensortower-market-research)** | Market research & competitive analysis | ✅ Active | [README](sensortower/README.md) |
| **[Game Health Check](#3-game-health-check)** | Daily KPI monitoring & diagnostics | ✅ Active (10 games) | [README](game_health_check/README.md) |
| **[Rolling Forecast](#4-rolling-forecast)** | Revenue & DAU forecasting | ✅ Active (11 games) | [README](rolling_forecast/README.md) |

---

## 1. Re-standardization

### Overview
Migrate historical game data from legacy GDS schemas to new standardized TSN Postgres schema.

### Use Case
- **Problem:** Games have different data schemas in GDS (old system)
- **Solution:** Extract → Transform → Load to unified schema in TSN
- **Benefit:** Consistent reporting, historical data preservation, cross-game analytics

### Key Features
- ✅ Three-stage pipeline (ETL → STD → CONS)
- ✅ Supports both Trino and Postgres sources
- ✅ Incremental daily processing
- ✅ Cumulative metrics calculation

### Quick Start
```bash
# Run for L2M game
./run_etl_process.sh \
    --layout layouts/re-standardization/etl/l2m_recharge.json \
    --gameId l2m \
    --logDate 2024-12-25

# Or use Airflow
airflow dags trigger re_standardization_l2m
```

### Architecture
```
GDS Trino/Postgres → ETL (HDFS Parquet) → STD (TSN Postgres) → CONS (Aggregated Tables)
```

### Current Games
- ✅ **L2M** - Complete (recharge, active, daily_revenue, cumulative)
- 🔄 **CFT** - Planned
- 🔄 **CS** - Planned

### How to Add New Game
**Detailed guide:** [layouts/re-standardization/README.md](re-standardization/README.md)

**Quick steps:**
1. Identify GDS data sources (Trino tables/Postgres schemas)
2. Create SQL extraction queries in `sql/`
3. Create layout files for ETL/STD/CONS stages
4. Add game to `GAMES_TO_RESTANDARDIZE` list in DAG
5. Test and deploy

### DAG
- **Name:** `re_standardization_{gameId}` (e.g., `re_standardization_l2m`)
- **Schedule:** Daily at 5 AM
- **File:** [dags/re_standardization_dag.py](../dags/re_standardization_dag.py)

---

## 2. SensorTower Market Research

### Overview
Extract and analyze mobile game market data from SensorTower API for competitive intelligence.

### Use Case
- **Problem:** Need insights on competitor games, market trends, new releases
- **Solution:** Automated API extraction + data processing pipeline
- **Benefit:** Track top games, monitor new releases, analyze performance metrics

### Key Features
- ✅ Multi-country support (VN, TH, ID, PH, SG, etc.)
- ✅ Parallel API extraction with rate limiting
- ✅ Auto-platform detection (iOS/Android)
- ✅ Metadata enrichment (genre, tags, publisher)

### Quick Start
```bash
# Step 1: Extract from API
./run_api_extraction.sh \
    --layout layouts/sensortower/raw/top_games.json \
    --month 2024-12

# Step 2: Process to parquet
./run_etl_process.sh \
    --layout layouts/sensortower/etl/top_games.json \
    --month 2024-12

# Step 3: Standardize
./run_etl_process.sh \
    --layout layouts/sensortower/std/top_games.json \
    --month 2024-12
```

### Architecture
```
SensorTower API → Raw JSON (HDFS) → ETL Parquet → STD Parquet → TSN Postgres
```

### Data Types
1. **Top Games** - Rankings by country/category
2. **New Games** - Newly released apps
3. **Metadata** - App details (publisher, genre, tags)
4. **Performance** - Downloads, revenue, DAU/MAU

### API Client Features
- Batch processing (100 apps per batch)
- Parallel requests (configurable workers)
- Automatic retry with backoff
- Platform auto-detection
- Country aggregation

### How to Add New Endpoint
1. Add API method to `src/core/api_client.py`
2. Create extraction layout in `layouts/sensortower/raw/`
3. Create ETL layout in `layouts/sensortower/etl/`
4. Add task to `dags/sensortower_dag.py`

### DAG
- **Name:** `sensortower_market_research`
- **Schedule:** Monthly (1st day at 2 AM)
- **File:** [dags/sensortower_dag.py](../dags/sensortower_dag.py)

### Documentation
- **Project Guide:** [layouts/sensortower/README.md](sensortower/README.md)
- **API Reference:** [docs/SENSORTOWER_QUICK_REFERENCE.md](../docs/SENSORTOWER_QUICK_REFERENCE.md)

---

## 3. Game Health Check

### Overview
Monitor daily game KPIs and generate health diagnostic reports for business teams.

### Use Case
- **Problem:** Need daily visibility into game performance (DAU, revenue, retention)
- **Solution:** Automated extraction + SQL consolidation + Google Sheets export
- **Benefit:** Early warning system, trend analysis, executive dashboards

### Key Features
- ✅ Standardized VIP tier calculation across all games
- ✅ SQL-based consolidation (no Python processing)
- ✅ Three report types: Diagnostic, Package Performance, Server Performance
- ✅ Auto-export to Google Sheets

### Quick Start
```bash
# Run complete pipeline for FW2
./run_game_health_pipeline.sh 2025-01-15 fw2

# Run specific report only
./run_game_health_pipeline.sh 2025-01-15 fw2 diagnostic

# Run all games, all reports
./run_game_health_pipeline.sh 2025-01-15
```

### Architecture
```
GDS Postgres → ETL (HDFS) → SQL Consolidation → Staging Tables → Google Sheets
```

### Data Sources (Phase 1: ETL)
1. **active_details** - Daily role activity
2. **charge_details** - Daily revenue with gross metrics
3. **user_profile** - Lifetime user metrics
4. **campaign** - Campaign metadata

### Reports (Phase 2: CONS)
1. **diagnostic_daily** - Campaign performance with VIP breakdown
2. **package_performance** - Package sales by server/VIP/product
3. **server_performance** - Server metrics (daily/weekly/monthly)

### Supported Games
cft, fw2, mlb, mts, td, gnoth, slth, pwmsea, cloudsongsea, l2m

### Standardized VIP Tiers
All games use same calculation:
```sql
CASE 
    WHEN total_rev = 0 THEN 'Free'
    WHEN total_rev / 25840 >= 20000 THEN 'G. >=20,000$'
    WHEN total_rev / 25840 >= 4000 THEN 'F. 4,000$ - 20,000$'
    WHEN total_rev / 25840 >= 2000 THEN 'E. 2,000$ - 4000$'
    WHEN total_rev / 25840 >= 1000 THEN 'D. 1,000$ - 2000$'
    WHEN total_rev / 25840 >= 200 THEN 'C. 200$ - 1,000$'
    WHEN total_rev / 25840 >= 20 THEN 'B. 20$ - 200$'
    ELSE 'A. <20$'
END
```

### How to Add New Game
1. Create ETL layouts in `layouts/game_health_check/etl/`
2. Update SQL files in `layouts/game_health_check/sql/` (or use templates)
3. Create CONS layouts in `layouts/game_health_check/cons/`
4. Add to `GAMES` list in `run_game_health_pipeline.sh`
5. Add Google Sheet ID to `configs/game_configs/game_name.yaml`
6. DAG will auto-discover from YAML

### DAGs
- **Names:** `game_health_check_{gameId}` (10 DAGs auto-created)
- **Schedule:** Daily at 3 AM
- **File:** [dags/game_health_check_dag.py](../dags/game_health_check_dag.py)
- **Auto-loads games from:** [configs/game_configs/game_name.yaml](../configs/game_configs/game_name.yaml)

### Documentation
- **Project Guide:** [layouts/game_health_check/README.md](game_health_check/README.md)
- **Jinja2 Templates:** [JINJA2_GUIDE.md](../JINJA2_GUIDE.md) (optional)

---

## 4. Rolling Forecast

### Overview
Generate daily revenue and DAU forecasts based on actual game metrics, with Google Sheets integration for business review.

### Use Case
- **Problem:** Need daily forecast updates for revenue planning
- **Solution:** Extract metrics → SQL consolidation → Export to Google Sheets
- **Benefit:** Real-time actuals for business planning, retention tracking, country-level granularity

### Key Features
- ✅ Three market types (single/sea/multi)
- ✅ Country-level granularity
- ✅ Retention metrics (NRU, NRU1, NRU7)
- ✅ Currency conversion support
- ✅ Auto-export to business Google Sheets

### Quick Start
```bash
# Single market game (Thailand only)
./run_rfc_pipeline.sh slth 2025-01-15 single

# SEA aggregate + countries
./run_rfc_pipeline.sh pwmsea 2025-01-15 sea

# Multi-country (per country reporting)
./run_rfc_pipeline.sh cft 2025-01-15 multi
```

### Architecture
```
GDS Postgres → ETL (HDFS) → SQL Consolidation → staging.rfc_daily_{gameId} → Google Sheets
```

### Market Types

| Type | Games | Output |
|------|-------|--------|
| **Single** | slth, gnoth | One row (TH only) |
| **SEA** | pwmsea, omgthai | SEA aggregate + optional countries |
| **Multi** | cft, fw2, mlb, mts, cloudsongsea, td, l2m | One row per country |

### Data Extracted (Phase 1: ETL)
1. **active** - Install, DAU, PU, NPU metrics
2. **retention** - NRU, NRU1, NRU7 cohorts
3. **charge** - Revenue data
4. **currency_mapping** - Exchange rates

### Consolidation (Phase 2: CONS)
- Joins all metrics by date/country
- Applies market-specific logic
- Converts currency to USD
- Outputs to `staging.rfc_daily_{gameId}`

### Supported Games
cft, fw2, mlb, mts, td, gnoth, slth, pwmsea, cloudsongsea, omgthai, l2m (11 total)

### How to Add New Game
1. Add game to `configs/game_configs/game_name.yaml` with `rfc_sheet_id`
2. Add to `GAMES_CONFIG` in `dags/rolling_forecast/consolidated_rolling_forecast_dag.py`
3. Determine market type (single/sea/multi)
4. Test with `./run_rfc_pipeline.sh {gameId} {date} {market_type}`

### DAGs
- **Names:** `{gameId}_daily_actual_rfc` (11 DAGs)
- **Schedule:** Staggered (3 AM - 1 PM GMT+7)
- **File:** [dags/rolling_forecast/consolidated_rolling_forecast_dag.py](../dags/rolling_forecast/consolidated_rolling_forecast_dag.py)
- **Auto-loads games from:** `GAMES_CONFIG` dictionary in DAG file

### Documentation
- **Project Guide:** [layouts/rolling_forecast/README.md](rolling_forecast/README.md)
- **Jinja2 Templates:** [JINJA2_GUIDE.md](../JINJA2_GUIDE.md) (optional)

---

## 🛠️ How to Create Layouts

### General Principles

1. **Choose the right stage:**
   - `etl/` - Extract from source, light transformation
   - `std/` - Standardize schema, add metadata
   - `cons/` - Multi-source joins, complex calculations

2. **Simple transformations → Use `schemas` array**
   - Column mapping
   - Type casting
   - Filtering
   - Grouping

3. **Complex joins → Use SQL files**
   - Multi-source joins
   - Window functions
   - Cumulative calculations

4. **Template variables:**
   - `{gameId}` - Game identifier
   - `{logDate}` - Processing date (YYYY-MM-DD)
   - `{month}` - Month (YYYY-MM)

### Layout Template

```json
{
  "description": "Clear description of what this layout does",
  
  "inputSources": [
    {
      "name": "data_source_name",
      "type": "jdbc|file",
      "connection": "GDS_TRINO|GDS_POSTGRES|TSN_POSTGRES",
      "query": "SELECT ... WHERE ds = '{logDate}'",
      "path": "hdfs://c0s/.../path",
      "format": "parquet|json|csv"
    }
  ],
  
  "sqlFile": "sql/optional_complex_query.sql",
  
  "schemas": [{
    "comment": "Optional for simple transformations",
    "columns": [
      {"colName": "column_name", "colType": "string|int|double|date|timestamp"},
      {"colName": "new_col", "source": "old_col", "colType": "double"}
    ],
    "filters": ["condition1", "condition2"],
    "groupBy": ["col1", "col2"],
    "orderBy": ["col1 DESC"]
  }],
  
  "output": {
    "type": "file|jdbc",
    "path": "hdfs://c0s/.../path/{logDate}",
    "format": "parquet",
    "numPartitions": 4,
    "table": "public.table_name",
    "mode": "append|overwrite",
    "deleteCondition": "ds = '{logDate}'"
  }
}
```

---

## 🎯 How to Create DAGs

### Option 1: Add to Existing Factory DAG

**Re-standardization:**
```python
# Edit dags/re_standardization_dag.py
GAMES_TO_RESTANDARDIZE = ['l2m', 'cft', 'new_game']  # Add here
```

**Game Health Check:**
```yaml
# Add to configs/game_configs/game_name.yaml
new_game:
  - "*":
      country_code: "*"
      game_name: "New Game Name"
      rfc_sheet_id: "your_google_sheet_id"
```

**Rolling Forecast:**
```python
# Edit dags/rolling_forecast/consolidated_rolling_forecast_dag.py
GAMES_CONFIG = {
    'new_game': ('0 3 * * *', datetime(2025, 1, 6), 'multi'),
}
```

### Option 2: Create New DAG from Scratch

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'gsbkk',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'my_new_pipeline',
    default_args=default_args,
    description='My new data pipeline',
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    catchup=False,
    tags=['my_project', 'etl'],
)

task1 = BashOperator(
    task_id='extract_data',
    bash_command="""
        cd /opt/airflow/dags/repo/gsbkk
        ./run_etl_process.sh \
            --layout layouts/my_project/etl/data.json \
            --gameId {{ dag_run.conf.get('gameId', 'default') }} \
            --logDate {{ ds }}
    """,
    dag=dag,
)

task2 = BashOperator(
    task_id='process_data',
    bash_command="""
        cd /opt/airflow/dags/repo/gsbkk
        ./run_etl_process.sh \
            --layout layouts/my_project/cons/output.json \
            --gameId {{ dag_run.conf.get('gameId', 'default') }} \
            --logDate {{ ds }}
    """,
    dag=dag,
)

task1 >> task2
```

---

## 📁 Project Directory Standards

```
layouts/{project_name}/
├── README.md              # Project-specific guide
├── etl/                   # Extract & Transform layouts
│   ├── {table1}.json
│   └── {table2}.json
├── std/                   # Standardization layouts (optional)
│   ├── {table1}.json
│   └── {table2}.json
├── cons/                  # Consolidation layouts
│   └── {report}.json
└── sql/                   # SQL files for complex queries
    ├── {table1}_etl.sql
    ├── {table2}_etl.sql
    └── {report}_cons.sql
```

---

## 🧪 Testing Checklist

Before deploying a new project/game:

- [ ] Layout JSON syntax is valid
- [ ] SQL queries tested in database console
- [ ] Template variables replaced correctly ({gameId}, {logDate})
- [ ] ETL produces non-empty output
- [ ] Data types match expectations
- [ ] No duplicate rows in output
- [ ] Performance is acceptable (< 30 min)
- [ ] DAG appears in Airflow UI
- [ ] Test run completes successfully

### Test Commands

```bash
# Test single layout
./run_etl_process.sh \
    --layout layouts/my_project/etl/test.json \
    --gameId test \
    --logDate 2024-12-25

# Verify HDFS output
hdfs dfs -ls hdfs://c0s/.../test/etl/test/2024-12-25
hdfs dfs -cat hdfs://c0s/.../test/etl/test/2024-12-25/*.parquet | head -10

# Verify Postgres output
psql -h tsn-postgres -d gsbkk -c "SELECT * FROM public.my_table LIMIT 5;"

# Test DAG
airflow dags test my_new_pipeline 2024-12-25
```

---

## 📚 Additional Resources

- **Main README:** [README.md](../README.md)
- **Layout Format Guide:** [layouts/README.md](README.md)
- **Jinja2 Templates:** [JINJA2_GUIDE.md](../JINJA2_GUIDE.md)
- **Quick Start Examples:** [QUICKSTART.md](../QUICKSTART.md)
- **Shell Scripts Guide:** Ask for explanation of shell scripts
- **Deployment Guide:** [docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md)

---

## 🎓 Learning Path

**For New Team Members:**

1. **Start here:** Read [README.md](../README.md) for overview
2. **Understand layouts:** Review [layouts/README.md](README.md)
3. **Pick a project:** Read project-specific README (this doc)
4. **Run examples:** Follow Quick Start in each project README
5. **Create test layout:** Add a simple ETL layout for test game
6. **Deploy to Airflow:** Add to existing DAG factory

**For Adding New Games:**

1. **Re-standardization:** [layouts/re-standardization/README.md](re-standardization/README.md) - Most detailed guide
2. **Game Health Check:** [layouts/game_health_check/README.md](game_health_check/README.md)
3. **Rolling Forecast:** [layouts/rolling_forecast/README.md](rolling_forecast/README.md)

**For Advanced Features:**

1. **Jinja2 Templates:** [JINJA2_GUIDE.md](../JINJA2_GUIDE.md)
2. **SQL Optimization:** Check project-specific SQL files
3. **Custom Transformations:** Review existing layouts for patterns

---

## 💡 Best Practices

### Layout Design
- ✅ One layout = one clear responsibility
- ✅ Use descriptive file names
- ✅ Add comments in JSON
- ✅ Test with small date ranges first

### SQL Files
- ✅ Use CTEs for readability
- ✅ Add partition filters early
- ✅ Handle NULL values explicitly
- ✅ Include data quality checks

### DAG Configuration
- ✅ Use factory pattern for similar DAGs
- ✅ Set appropriate retry policies
- ✅ Tag DAGs properly for filtering
- ✅ Document schedule rationale

### Performance
- ✅ Increase partitions for large data
- ✅ Use column pruning (select only needed columns)
- ✅ Add filters early in SQL
- ✅ Monitor execution time

---

## 🆘 Troubleshooting

**Layout not found:**
- Check file path is absolute or relative to repo root
- Verify JSON syntax with online validator

**Connection errors:**
- Check credentials in HDFS (`hdfs://c0s/.../configs/cred_*.json`)
- Verify connection name matches config

**Empty output:**
- Check date filter in SQL
- Verify source data exists for that date
- Review filter conditions

**Performance issues:**
- Increase `numPartitions`
- Add partition filters in SQL
- Check for Cartesian joins

**DAG not appearing:**
- Check `globals()[dag_name] = dag` line exists
- Verify DAG file has no Python syntax errors
- Refresh Airflow UI

---

**Last Updated:** December 26, 2025  
**Maintained by:** GSBKK Team
