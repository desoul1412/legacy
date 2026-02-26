# SensorTower Market Research Layouts

This directory contains layout JSON files for processing SensorTower market research data.

## Architecture Overview

The market research pipeline follows a **2-step architecture**:

### Step 1: API Extraction (Python Script)
- **Script**: `src/pipelines/market_research.py`
- **Output**: Raw JSON data in HDFS
- **Location**: `hdfs://c0s/user/gsbkk-workspace-yc9t6/sensortower/raw/`

### Step 2: ETL Processing (Layout Files)
- **Layouts**: Files in this directory organized by transformation stage
- **Process**: raw → etl → std → cons
- **Location**: HDFS paths defined in each layout file

## Directory Structure

```
layouts/sensortower/
├── etl/                      # Raw JSON → ETL (Parquet)
│   ├── top_games.json
│   ├── new_games.json
│   ├── metadata.json
│   └── performance.json
├── std/                      # ETL → Standardized
│   ├── top_games.json
│   ├── new_games.json
│   ├── metadata.json
│   └── performance.json
└── cons/                     # STD → Consolidated
    └── market_insights.json
```

## Layout Files by Stage

### ETL Stage (raw → etl)

Converts raw JSON to parquet format for efficient processing.

#### etl/top_games.json
- **Input**: `sensortower/raw/top_games/{country}/{logDate}` (JSON)
- **Output**: `sensortower/etl/top_games/{country}/{logDate}` (Parquet)
- **Purpose**: Top games by country rankings

#### etl/new_games.json
- **Input**: `sensortower/raw/new_games/{country}/{logDate}` (JSON)
- **Output**: `sensortower/etl/new_games/{country}/{logDate}` (Parquet)
- **Purpose**: Newly released games

#### etl/metadata.json
- **Input**: `sensortower/raw/metadata/{logDate}` (JSON)
- **Output**: `sensortower/etl/metadata/{logDate}` (Parquet)
- **Purpose**: App metadata (publisher, category, version, etc.)

#### etl/performance.json
- **Input**: `sensortower/raw/performance/{logDate}` (JSON)
- **Output**: `sensortower/etl/performance/{logDate}` (Parquet)
- **Purpose**: Performance metrics (downloads, revenue, DAU, MAU)

### STD Stage (etl → std)

Standardizes data with consistent schemas and data types.

#### std/top_games.json
- **Input**: `sensortower/etl/top_games/{country}/{logDate}` (Parquet)
- **Output**: `sensortower/std/top_games/{logDate}` (Parquet)
- **Schema**: app_id, app_name, publisher_name, country, category, rank, downloads, revenue
- **Filters**: Non-null app_id and app_name

#### std/new_games.json
- **Input**: `sensortower/etl/new_games/{country}/{logDate}` (Parquet)
- **Output**: `sensortower/std/new_games/{logDate}` (Parquet)
- **Schema**: app_id, app_name, publisher_name, country, category, release_date, initial_downloads
- **Filters**: Non-null app_id, app_name, and release_date

#### std/metadata.json
- **Input**: `sensortower/etl/metadata/{logDate}` (Parquet)
- **Output**: `sensortower/std/metadata/{logDate}` (Parquet)
- **Schema**: app_id, app_name, publisher_name, publisher_id, category, release_date, description, version, size_bytes
- **Filters**: Non-null app_id

#### std/performance.json
- **Input**: `sensortower/etl/performance/{logDate}` (Parquet)
- **Output**: `sensortower/std/performance/{logDate}` (Parquet)
- **Schema**: app_id, country, date, downloads, revenue, daily_active_users, monthly_active_users
- **Filters**: Non-null app_id and country

### CONS Stage (std → cons)

Consolidates all standardized data into final reporting format.

#### cons/market_insights.json
- **Input**: `sensortower/std/*/{logDate}` (All tables, Parquet)
- **Output**: `sensortower/cons/market_insights/{logDate}` (Parquet)
- **Purpose**: Comprehensive market insights report
- **Aggregations**:
  - Total downloads and revenue across countries
  - Average and best rankings
  - List of countries where app appears
  - App metadata (name, publisher, category, release date)
- **Group By**: app_id
- **Order By**: total_revenue DESC, total_downloads DESC

## DAG Usage Example

```python
# Step 1: API Extraction
pipelines_extraction = {
    'extract_top_games': {
        'action': 'market_research',
        'step': 'extract_top_games',
        **api_conf
    }
}

# Step 2: ETL Processing
pipelines_etl = {
    # Raw → ETL
    'raw_to_etl_top_games': {
        'action': 'etl',
        'layouts': 'sensortower/etl/top_games',
        **etl_conf
    },
    
    # ETL → STD
    'etl_to_std_top_games': {
        'action': 'etl',
        'layouts': 'sensortower/std/top_games',
        **etl_conf
    },
    
    # STD → CONS
    'std_to_cons_market_insights': {
        'action': 'etl',
        'layouts': 'sensortower/cons/market_insights',
        **etl_conf
    }
}
```

See `dags/sensortower/sensortower_dag.py` for a complete implementation example.

## Data Flow

```
SensorTower API
    ↓ extract_top_games, extract_new_games, extract_metadata, extract_performance
Raw JSON
├── sensortower/raw/top_games/{country}/{date}
├── sensortower/raw/new_games/{country}/{date}
├── sensortower/raw/metadata/{date}
└── sensortower/raw/performance/{date}
    ↓ etl/{table}.json
ETL Parquet
├── sensortower/etl/top_games/{country}/{date}
├── sensortower/etl/new_games/{country}/{date}
├── sensortower/etl/metadata/{date}
└── sensortower/etl/performance/{date}
    ↓ std/{table}.json
Standardized
├── sensortower/std/top_games/{date}
├── sensortower/std/new_games/{date}
├── sensortower/std/metadata/{date}
└── sensortower/std/performance/{date}
    ↓ cons/market_insights.json
Consolidated Report
└── sensortower/cons/market_insights/{date}
```

## Notes

- **Date Format**: Use YYYY-MM format (e.g., "2025-01")
- **Countries**: VN, TH, US (configured in API extraction step)
- **Partitions**: Adjust `numPartitions` in layouts based on data volume
- **Variables**: Automatically substituted from DAG template (e.g., {logDate}, {country})
- **Testing**: Always test new layouts with a small date range first

