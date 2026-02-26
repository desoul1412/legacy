# GSBKK Data Pipeline 🎮📊

> **A modular ETL framework for game analytics at scale**

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Creating Layouts](#creating-layouts)
5. [Creating DAGs](#creating-dags)
6. [Projects](#projects)
7. [Case Study: Sensor Tower](#case-study-sensor-tower)

---

## Project Overview

### What is GSBKK?

GSBKK is a **layout-driven ETL framework** that transforms game data from multiple sources into standardized analytics tables. It supports:

- **Database ETL**: PostgreSQL, Trino → HDFS Parquet → PostgreSQL
- **API Extraction**: SensorTower, Facebook Ads, TikTok Ads
- **Google Sheets**: Bi-directional sync for business reports

### Why Layout-Based?

Instead of writing repetitive Python code for each data pipeline, GSBKK uses **JSON layout files** that declare:
- Input sources (JDBC, files, Google Sheets)
- SQL transformations (Jinja2 templates)
- Output destinations (HDFS, PostgreSQL, Google Sheets)

**Benefits**:
- 70-90% code reduction vs traditional ETL
- Non-programmers can create new pipelines
- Guaranteed consistency across all games
- Easy to test and version control

### Core Projects

| Project | Purpose | Schedule | Output |
|---------|---------|----------|--------|
| **Game Health Check** | Daily KPI monitoring (DAU, revenue, VIP) | Daily 3-7 AM | TSN Postgres |
| **Rolling Forecast** | Revenue predictions + Google Sheets import | Daily 4 AM, 9 AM | Google Sheets + PostgreSQL |
| **Re-Standardization** | Migrate legacy game formats | On-demand | TSN Postgres |
| **SensorTower** | Competitive market research | Monthly 1st | TSN Postgres |

---

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
├─────────────────┬─────────────────┬─────────────────┬───────────────┤
│  GDS Postgres   │   GDS Trino     │  External APIs  │ Google Sheets │
│  (game data)    │  (data lake)    │  (SensorTower)  │  (business)   │
└────────┬────────┴────────┬────────┴────────┬────────┴───────┬───────┘
         │                 │                 │                │
         ▼                 ▼                 ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ETL ENGINE                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │   Layout    │  │   SQL       │  │  PySpark    │                  │
│  │   Parser    │──│  Renderer   │──│  Executor   │                  │
│  │   (JSON)    │  │  (Jinja2)   │  │  (DataFrame)│                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        DESTINATIONS                                  │
├─────────────────┬─────────────────┬─────────────────────────────────┤
│   HDFS Parquet  │   TSN Postgres  │      Google Sheets              │
│   (staging)     │   (production)  │      (reports)                  │
└─────────────────┴─────────────────┴─────────────────────────────────┘
```

### Core Components

```
gsbkk/
├── src/
│   ├── etl_engine.py        # Main ETL processor (reads layouts, executes transforms)
│   ├── api_extractor.py     # API data extraction (SensorTower, etc.)
│   ├── gsheet_processor.py  # Google Sheets bi-directional sync
│   └── core/
│       ├── models.py        # Type-safe dataclasses (LayoutConfig, InputSource)
│       ├── loaders.py       # JDBC connections, file loading utilities
│       ├── templates.py     # SQL template rendering (Jinja2)
│       └── writers.py       # Output writers (HDFS, Postgres, GSheet)
│
├── layouts/                 # JSON layout files (THE HEART OF THE SYSTEM)
│   ├── game_health_check/   # ETL → STD → CONS for each game
│   ├── rolling_forecast/    # Reuses health check data
│   ├── re-standardization/  # Legacy game migration
│   └── sensortower/         # Market research pipeline
│
├── templates/               # Jinja2 SQL templates
│   └── sql/                 # Reusable macros (VIP calc, currency, etc.)
│
├── dags/                    # Airflow DAG definitions
│   ├── utils/dag_helpers.py # Task factory functions
│   └── {project}/           # Project-specific DAGs
│
└── configs/                 # Configuration files
    └── game_configs/        # Per-game settings
```

### ETL Pipeline Stages

Every project follows a **3-stage pattern**:

```
┌─────────┐      ┌─────────┐      ┌─────────┐
│   ETL   │ ──▶  │   STD   │ ──▶  │  CONS   │
│ Extract │      │Standardize     │Consolidate
│ raw data│      │ transform │    │ & write  │
└─────────┘      └─────────┘      └─────────┘
     ▼                ▼                ▼
   HDFS            HDFS           PostgreSQL
  (raw/)          (std/)          (public.*)
```

---

## Quick Start

### Prerequisites

```bash
# Python 3.8+
pip install -r requirements.txt

# Spark 3.x (for local testing)
# HDFS access configured
```

### Run Your First Pipeline

```bash
# 1. Single layout execution
./run_etl_process.sh \
    --layout layouts/game_health_check/gnoth/etl/active_details.json \
    --gameId gnoth \
    --logDate 2026-01-11

# 2. Full game health check (all stages)
./run_game_health_pipeline.sh 2026-01-11 gnoth

# 3. Rolling forecast
./run_rfc_pipeline.sh gnoth 2026-01-11 single
```

### Available Scripts

| Script | Purpose | Example |
|--------|---------|---------|
| `run_etl_process.sh` | Execute single layout | `--layout X --gameId Y --logDate Z` |
| `run_api_extraction.sh` | API data extraction | `sensortower top_games 2024-12` |
| `run_gsheet_process.sh` | Google Sheets sync | `--layout X --direction read/write` |
| `run_game_health_pipeline.sh` | Full health check | `<date> [game] [report]` |
| `run_rfc_pipeline.sh` | Rolling forecast | `<game> <date> <market_type>` |

---

## Creating Layouts

### Layout Structure

A layout JSON file defines a complete ETL pipeline:

```json
{
  "description": "Human-readable description",
  
  "inputSources": [
    {
      "name": "source_data",           // Temp view name in Spark
      "type": "jdbc",                  // jdbc | file | gsheet
      "connection": "GDS_POSTGRES",    // Connection name
      "sqlTemplate": "templates/path/query.sql.j2"
    }
  ],
  
  "sqlTemplate": "templates/path/transform.sql.j2",  // Optional: SQL after loading
  
  "outputs": [
    {
      "type": "file",                  // file | jdbc | gsheet
      "path": "hdfs://c0s/path/{logDate}",
      "format": "parquet"
    },
    {
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "table": "public.my_table",
      "mode": "overwrite"
    }
  ]
}
```

### Input Source Types

#### 1. JDBC (Database)

```json
{
  "name": "active_data",
  "type": "jdbc",
  "connection": "GDS_POSTGRES",        // or GDS_TRINO, TSN_POSTGRES
  "sqlTemplate": "templates/sql/query.sql.j2",
  "tableKey": "daily_active"           // Optional: resolves to game-specific table
}
```

**Available Connections**:
- `GDS_POSTGRES`: Game data source (schema.table format)
- `GDS_TRINO`: Data lake (catalog.schema.table format)
- `TSN_POSTGRES`: Analytics destination

#### 2. File (HDFS/Local)

```json
{
  "name": "etl_data",
  "type": "file",
  "path": "hdfs://c0s/user/gsbkk/data/{gameId}/{logDate}",
  "format": "parquet"                  // parquet | json | csv
}
```

#### 3. Google Sheets

```json
{
  "name": "gsheet_data",
  "type": "gsheet",
  "sheet_id": "1ABC...xyz",
  "range": "Sheet1!A:Z"
}
```

### Output Types

#### File Output (HDFS)

```json
{
  "type": "file",
  "path": "hdfs://c0s/user/gsbkk/output/{gameId}/{logDate}",
  "format": "parquet",
  "numPartitions": 4,
  "mode": "overwrite"
}
```

#### JDBC Output (PostgreSQL)

```json
{
  "type": "jdbc",
  "connection": "TSN_POSTGRES",
  "table": "public.ghc_diagnostic_daily",
  "mode": "overwrite",
  "deleteCondition": "game_id = '{gameId}' AND log_date = '{logDate}'",
  "add_updated_at": true
}
```

#### Google Sheets Output

```json
{
  "type": "gsheet",
  "sheet_id": "1ABC...xyz",
  "range": "Data!A:Z",
  "columns": ["date", "revenue", "dau"],
  "update_mode": "append"
}
```

### Variable Substitution

Variables in `{curly braces}` are replaced at runtime:

| Variable | Source | Example |
|----------|--------|---------|
| `{gameId}` | `--gameId` argument | `gnoth` |
| `{logDate}` | `--logDate` argument | `2026-01-11` |
| `{sqlQuery}` | Rendered SQL template | Full SQL string |
| `{table_path}` | Resolved from `tableKey` | `common_tables_2.daily_role_active_details` |

### SQL Templates (Jinja2)

Templates support variables, macros, and conditionals:

```sql
-- templates/game_health_check/gnoth/active_details.sql.j2
SELECT 
    role_id,
    server_id,
    report_date,
    {{ calculate_vip_level('total_revenue') }} as vip_level
FROM {{ table_path }}
WHERE report_date = '{{ logDate }}'
```

**Available Macros** (in `templates/sql/macros.sql`):
- `calculate_vip_level(column)` - VIP tier from revenue
- `convert_currency(amount, rate)` - Currency conversion
- `safe_divide(a, b)` - Division with zero handling

### Complete Layout Example

```json
{
  "description": "GNOTH - Daily diagnostic aggregation",
  
  "inputSources": [
    {
      "name": "active_data",
      "type": "file",
      "path": "hdfs://c0s/user/gsbkk/game_health_check/gnoth/std/active/{logDate}",
      "format": "parquet"
    },
    {
      "name": "charge_data", 
      "type": "file",
      "path": "hdfs://c0s/user/gsbkk/game_health_check/gnoth/std/charge/{logDate}",
      "format": "parquet"
    }
  ],
  
  "sqlTemplate": "templates/game_health_check/gnoth/cons/diagnostic_daily.sql.j2",
  
  "outputs": [
    {
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "table": "public.ghc_diagnostic_daily_gnoth",
      "mode": "overwrite",
      "deleteCondition": "log_date = '{logDate}'"
    }
  ]
}
```

---

## Creating DAGs

### DAG Structure

Each project has its own DAG file using helper functions:

```python
# dags/game_health_check/gnoth_game_health_check_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from utils.dag_helpers import create_pipeline_tasks

dag = DAG(
    dag_id='game_health_check_gnoth',
    default_args={
        'owner': 'gsbkk',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
    },
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    catchup=True,
    max_active_runs=1,
    tags=['game_health', 'gnoth'],
)

# Define pipeline as declarative config
pipeline = [
    # ETL Stage
    {'name': 'etl_active', 'type': 'etl', 
     'layout': 'layouts/game_health_check/gnoth/etl/active_details.json'},
    {'name': 'etl_charge', 'type': 'etl',
     'layout': 'layouts/game_health_check/gnoth/etl/charge_details.json'},
    
    # STD Stage
    {'name': 'std_active', 'type': 'etl',
     'layout': 'layouts/game_health_check/gnoth/std/active.json'},
    
    # CONS Stage
    {'name': 'cons_diagnostic', 'type': 'etl',
     'layout': 'layouts/game_health_check/gnoth/cons/diagnostic_daily.json'},
]

# Create all tasks from config
tasks = create_pipeline_tasks(dag, pipeline, game_id='gnoth', log_date='{{ ds }}')

# Define dependencies
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

start >> [tasks['etl_active'], tasks['etl_charge']]
[tasks['etl_active'], tasks['etl_charge']] >> tasks['std_active']
tasks['std_active'] >> tasks['cons_diagnostic']
tasks['cons_diagnostic'] >> end
```

### DAG Helper Functions

Located in `dags/utils/dag_helpers.py`:

| Function | Purpose | Task Type |
|----------|---------|-----------|
| `create_etl_operator()` | Database/file ETL | `etl` |
| `create_api_operator()` | API extraction | `api` |
| `create_gsheet_operator()` | Google Sheets sync | `gsheet` |
| `create_pipeline_operator()` | Python scripts | `pipeline` |
| `create_pipeline_tasks()` | Batch create from config | All |

### Pipeline Config Format

```python
pipeline = [
    # ETL type - uses run_etl_process.sh
    {
        'name': 'task_id',
        'type': 'etl',
        'layout': 'layouts/path/to/layout.json',
        'log_date': '{{ ds }}',        # Optional, default: {{ ds }}
        'game_id': 'gnoth',             # Optional, uses dag default
        'vars': 'key1=val1,key2=val2',  # Optional extra variables
    },
    
    # API type - uses run_api_extraction.sh
    {
        'name': 'extract_top_games',
        'type': 'api',
        'layout': 'layouts/sensortower/raw/top_games.json',
        'month': "{{ execution_date.strftime('%Y-%m') }}",
    },
    
    # GSheet type - uses run_gsheet_process.sh  
    {
        'name': 'push_to_sheets',
        'type': 'gsheet',
        'layout': 'layouts/rfc/gsheet/metrics.json',
        'ggsheet_id': '1ABC...xyz',
    },
    
    # Pipeline type - runs Python scripts
    {
        'name': 'custom_processing',
        'type': 'pipeline',
        'script': 'market_research',
        'params': ['extract_top_games', '{{ ds }}'],
    },
]
```

### Airflow Date Variables

| Variable | Meaning | Example (run at 3 AM Jan 12) |
|----------|---------|------------------------------|
| `{{ ds }}` | data_interval_start | `2026-01-11` |
| `{{ data_interval_end }}` | End of interval | `2026-01-12` |
| `{{ execution_date }}` | Same as ds (legacy) | `2026-01-11` |

**Important**: `{{ ds }}` gives you "yesterday" from the run time perspective. DAG scheduled at 3 AM processes previous day's data.

---

## Projects

### 1. Game Health Check

**Purpose**: Daily KPI monitoring for all games

**Flow**:
```
GDS Postgres ──▶ ETL (extract) ──▶ STD (transform) ──▶ CONS (load to TSN)
```

**Outputs**:
- `ghc_diagnostic_daily_{game}` - DAU, revenue, new users
- `ghc_package_performance_{game}` - Item sales by VIP
- `ghc_server_performance_{game}` - Server metrics

**Run**:
```bash
./run_game_health_pipeline.sh 2026-01-11 gnoth
```

### 2. Rolling Forecast

**Purpose**: Revenue predictions for business planning

**Flow** (reuses health check data):
```
TSN Postgres (ghc_*) ──▶ CONS ──▶ Google Sheets
```

**Market Types**:
- `single` - Thailand only (gnoth, slth)
- `sea` - SEA aggregate (pwmsea, omgthai)
- `multi` - Multi-country breakdown (fw2, l2m)

**Run**:
```bash
./run_rfc_pipeline.sh gnoth 2026-01-11 single
```

### 3. Re-Standardization

**Purpose**: Migrate legacy game formats to standard schema

**Run**:
```bash
./run_etl_process.sh --layout layouts/re-standardization/l2m/etl/recharge.json \
    --gameId l2m --logDate 2026-01-11
```

### 4. SensorTower

See detailed case study below.

---

## Case Study: Sensor Tower

### Overview

SensorTower provides competitive intelligence data about mobile game markets. This project extracts market research data via API and processes it into analytics tables.

### API Endpoints Used

| Endpoint | Purpose | URL |
|----------|---------|-----|
| **Top Games** | Revenue rankings by country | `/{os}/sales_report_estimates_comparison_attributes` |
| **New Games** | Recently released apps | `/{os}/apps/app_ids` |
| **App Details** | Metadata (publisher, category) | `/{device}/apps/get_app` |
| **Performance** | Revenue/downloads time series | `/{os}/sales_report_estimates` |

### Pipeline Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                     DISCOVERY PHASE                                │
├───────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │ extract_top  │      │ etl_top      │      │ std_top      │     │
│  │ _games (API) │ ───▶ │ _games       │ ───▶ │ _games       │     │
│  │ JSON → HDFS  │      │ JSON→Parquet │      │ Normalize    │     │
│  └──────────────┘      └──────────────┘      └──────┬───────┘     │
│                                                      │             │
│  ┌──────────────┐      ┌──────────────┐      ┌──────┴───────┐     │
│  │ extract_new  │      │ etl_new      │      │ std_new      │     │
│  │ _games (API) │ ───▶ │ _games       │ ───▶ │ _games       │ ──┐ │
│  │ JSON → HDFS  │      │ JSON→Parquet │      │ Normalize    │   │ │
│  └──────────────┘      └──────────────┘      └──────────────┘   │ │
│                                                                  │ │
│                                               ┌──────────────┐   │ │
│                                               │market_insights│◀─┘ │
│                                               │ Union all    │     │
│                                               │ → CONS       │     │
│                                               └──────────────┘     │
└───────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                     APP DETAILS PHASE                              │
├───────────────────────────────────────────────────────────────────┤
│  (Gets app_ids from CONS, fetches metadata)                       │
│                                                                    │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │ extract_     │      │ etl_         │      │ dim_apps     │     │
│  │ metadata     │ ───▶ │ metadata     │ ───▶ │ (dimension)  │     │
│  └──────────────┘      └──────────────┘      └──────────────┘     │
└───────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                   PERFORMANCE PHASE                                │
├───────────────────────────────────────────────────────────────────┤
│  (Gets first 90 days performance for new games)                   │
│                                                                    │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │ extract_     │      │ etl_daily    │      │ daily_       │     │
│  │ performance  │ ───▶ │ _performance │ ───▶ │ performance  │     │
│  │ (API batch)  │      │              │      │ (CONS)       │     │
│  └──────────────┘      └──────────────┘      └──────────────┘     │
└───────────────────────────────────────────────────────────────────┘
```

### Step-by-Step Explanation

#### Step 1: RAW Extraction (API → HDFS JSON)

**Script**: `src/apis/sensortower_api.py`
**Output**: `hdfs://.../sensortower/raw/top_games/{country}/{month}/`

```python
# Top Games API Call
client = SensorTowerAPI()
data = client.get_top_apps(
    start_date='2024-12-01',
    end_date='2024-12-31',
    region='VN',          # Country code
    os='ios'              # ios or android
)
# Returns: [{"app_id": "123", "name": "Game A", "revenue": 1000000, ...}, ...]
```

**API Parameters**:
```python
params = {
    'date': '2024-12-01',
    'end_date': '2024-12-31',
    'regions': 'VN',
    'category': '6014',              # Games category (iOS)
    'limit': 5,
    'measure': 'revenue',
    'comparison_attribute': 'absolute',
    'time_range': 'month',
    'auth_token': api_key
}
```

#### Step 2: ETL (JSON → Parquet)

**Layout**: `layouts/sensortower/etl/top_games.json`

```json
{
  "inputSources": [
    {
      "name": "source_data",
      "type": "file",
      "format": "json",
      "path": "hdfs://.../sensortower/raw/top_games/*/{logDate}"
    }
  ],
  "sqlTemplate": "templates/sensortower/etl_top_games.sql.j2",
  "outputs": [
    {
      "type": "file",
      "path": "hdfs://.../sensortower/etl/top_games/{logDate}",
      "format": "parquet"
    }
  ]
}
```

**SQL Transform**:
```sql
SELECT
    app_id,
    name,
    publisher_name,
    CAST(revenue AS DOUBLE) as revenue,
    CAST(downloads AS BIGINT) as downloads,
    country,
    '{{ logDate }}' as extract_month
FROM source_data
WHERE app_id IS NOT NULL
```

#### Step 3: STD (Normalize & Dedupe)

**Layout**: `layouts/sensortower/std/top_games.json`

Standardizes column names, applies business rules, removes duplicates.

#### Step 4: CONS (Consolidate to PostgreSQL)

**Layout**: `layouts/sensortower/cons/market_insights.json`

```json
{
  "inputSources": [
    {"name": "top_games", "type": "file", "path": "hdfs://.../std/top_games"},
    {"name": "new_games", "type": "file", "path": "hdfs://.../std/new_games"}
  ],
  "sqlTemplate": "templates/market_research/market_insights.sql.j2",
  "outputs": [
    {"type": "file", "path": "hdfs://.../cons/market_insights", "format": "parquet"}
  ]
}
```

### DAG Configuration

**File**: `dags/sensortower/sensortower_dag.py`

```python
dag = DAG(
    'sensortower_market_research',
    schedule_interval='0 2 1 * *',  # 2 AM on 1st of month
    ...
)

# Month template (previous month)
month_template = "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m') }}"

discovery_pipeline = [
    {'name': 'extract_top_games', 'type': 'pipeline',
     'script': 'market_research', 'params': ['extract_top_games', month_template]},
    {'name': 'etl_top_games', 'type': 'etl',
     'layout': 'layouts/sensortower/etl/top_games.json', 'log_date': month_template},
    {'name': 'std_top_games', 'type': 'etl',
     'layout': 'layouts/sensortower/std/top_games.json', 'log_date': month_template},
    {'name': 'market_insights', 'type': 'etl',
     'layout': 'layouts/sensortower/cons/market_insights.json', 'log_date': month_template},
]
```

### Final Output Tables

| Table | Description |
|-------|-------------|
| `st_market_insights` | Top & new games with revenue rankings |
| `st_dim_apps` | App dimension table (metadata) |
| `st_daily_performance` | Time-series revenue/downloads |

### Running Manually

```bash
# Step 1: Extract top games (API call)
./run_pipeline.sh market_research extract_top_games 2024-12

# Step 2: ETL transform
./run_etl_process.sh \
    --layout layouts/sensortower/etl/top_games.json \
    --logDate 2024-12

# Step 3: Standardize
./run_etl_process.sh \
    --layout layouts/sensortower/std/top_games.json \
    --logDate 2024-12

# Step 4: Consolidate
./run_etl_process.sh \
    --layout layouts/sensortower/cons/market_insights.json \
    --logDate 2024-12
```

---

## Documentation Index

| Document | Description |
|----------|-------------|
| [QUICKSTART.md](QUICKSTART.md) | 5-minute setup guide |
| [JINJA2_GUIDE.md](JINJA2_GUIDE.md) | SQL template system |
| [docs/ETL_ENGINE_GUIDE.md](docs/ETL_ENGINE_GUIDE.md) | Layout file reference and usage |
| [docs/GOOGLE_SHEETS_INTEGRATION.md](docs/GOOGLE_SHEETS_INTEGRATION.md) | Google Sheets input/output guide |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment |
| [layouts/game_health_check/README.md](layouts/game_health_check/README.md) | Game health monitoring |
| [layouts/rolling_forecast/README.md](layouts/rolling_forecast/README.md) | Revenue forecasting + Google Sheets import |
| [layouts/sensortower/README.md](layouts/sensortower/README.md) | Market research |
| [dags/README.md](dags/README.md) | DAG organization |

---

**Version**: 4.0 | **Team**: GSBKK | **Updated**: January 2026
