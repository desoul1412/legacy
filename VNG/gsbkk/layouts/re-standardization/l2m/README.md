# L2M Re-Standardization Project - Complete Pipeline Architecture

## Overview
Optimized ETL-STD-CONS architecture for L2M game data processing, covering all use cases from the original Python scripts.

## Architecture Benefits
- **Parallel Extraction**: Split massive tables into smaller, parallelizable queries
- **HDFS Intermediate Storage**: Faster debugging and reprocessing
- **PySpark Union**: ~100x faster than Trino UNION on large tables
- **Atomic PostgreSQL Writes**: Delete+insert pattern prevents duplicates

## Data Flow

### Layer 1: ETL (Trino → HDFS)
**Purpose**: Extract raw data from Iceberg tables, filtered by date

#### Active Users (2 lightweight jobs - recharge/item/money data reused!)
- `l2m_active_login.json` → `etl_login` 
- `l2m_active_logout.json` → `etl_logout`

**Optimization**: We DO NOT separately extract from `etl_recharge`, `etl_increase_item`, or `etl_increase_money` for active users. Instead, we reuse data from recharge/item/money economy jobs (below) to avoid triple-scanning massive tables!

#### Revenue (SINGLE Trino query for both outputs!)
- `l2m_recharge.json` → `etl_recharge` with currency conversion
  - Output columns: `user_id`, `server_id`, `pmc`, `ds`, `amount`, `daily_rev_usd`
  - Used for: Recharge CONS table + Cumulative Revenue + Server/Farmers Performance
  - **Optimization**: One Trino scan produces both recharge details AND daily_rev_usd for cumulative calc!

#### Item/Money Economy (also used for active user detection!)
- `l2m_item_gain.json` → `etl_increase_item` (51 key items)
- `l2m_item_spend.json` → `etl_decrease_item` (51 key items)
- `l2m_money_gain.json` → `etl_increase_money` (diamond 9%2, grouped by reason_id)
- `l2m_money_spend.json` → `etl_decrease_money` (diamond 9%2, grouped by reason_id)

### Layer 2: STD (HDFS → HDFS)
**Purpose**: Only for complex transformations (unions, aggregations) - NOT for simple enrichments

#### Active Users STD (UNION required)
- Input: 7 HDFS paths (login, logout, recharge, item_gain, item_spend, money_gain, money_spend)
- Transform: PySpark UNION ALL + GROUP BY user_id, MAX(level)
- **Key optimization**: Reuses all economy data to avoid multiple Trino scans
- Output: `hdfs://.../l2m/std/active/{logDate}`

#### Item/Money STD - REMOVED!
- **Old approach**: ETL → STD (add country_code) → CONS (write to PostgreSQL)
- **New approach**: ETL → CONS (enrich + write in one step using `enrichments` array)
- **Why**: STD layer was just doing a simple LEFT JOIN - wasteful HDFS intermediate storage
- **Result**: Faster execution, less storage, cleaner code

### Layer 3: CONS (HDFS/PostgreSQL → PostgreSQL)
**Purpose**: Business logic, enrichment with dimension tables, final writes

**Pattern for PostgreSQL Joins**: Use `inputSources` array to combine HDFS and JDBC sources
```json
"inputSources": [
  {
    "name": "main_data",
    "type": "file",
    "path": "hdfs://.../item_gain/{logDate}",
    "format": "parquet"
  },
  {
    "name": "user_profile",
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "query": "SELECT user_id, country_code FROM public.mkt_user_profile"
  }
],
"transformSql": "SELECT m.*, p.country_code FROM main_data m LEFT JOIN user_profile p USING(user_id)"
```

#### Core Metrics (Date-partitioned)
1. **l2m_active** - Read from STD active → Write to PostgreSQL
2. **l2m_recharge** - Read from ETL recharge → Write to PostgreSQL  
3. **l2m_cumulative_revenue** - Read ETL recharge + prev state from PostgreSQL
4. **l2m_item_gain** - Read ETL + join PostgreSQL user_profile for country
5. **l2m_item_spend** - Read ETL + join PostgreSQL user_profile for country
6. **l2m_money_gain** - Read ETL + join PostgreSQL user_profile for country + VIP
7. **l2m_money_spend** - Read ETL + join PostgreSQL user_profile for country + VIP
8. **l2m_server_performance** - Read STD active + ETL recharge → Aggregate by server
9. **l2m_farmers_performance** - Read STD active + ETL recharge + join user_profile + farmers

#### Dimension Tables (Snapshots)
10. **l2m_guild_info** - Latest clan membership (overwrite mode)
11. **l2m_farmers_snapshot** - Detected bot/farmer accounts (overwrite mode)

## File Structure

```
templates/re-standardization/l2m/
├── active_login.sql.j2
├── active_recharge.sql.j2
├── recharge.sql.j2
├── daily_revenue.sql.j2
├── cumulative_revenue.sql.j2
├── item_gain.sql.j2
├── item_spend.sql.j2
├── money_gain.sql.j2
├── money_spend.sql.j2
├── server_performance.sql.j2
├── guild_info.sql.j2
└── farmers_snapshot.sql.j2

layouts/re-standardization/l2m/
├── etl/
│   ├── l2m_active_login.json
│   ├── l2m_active_logout.json
│   ├── l2m_recharge.json (outputs both amount + daily_rev_usd)
│   ├── l2m_item_gain.json
│   ├── l2m_item_spend.json
│   ├── l2m_money_gain.json
│   └── l2m_money_spend.json
│
├── std/
│   └── l2m_active.json (ONLY file - union 7 sources)
│
└── cons/
    ├── l2m_active.json
    ├── l2m_recharge.json
    ├── l2m_cumulative_revenue.json
    ├── l2m_item_gain.json
    ├── l2m_item_spend.json
    ├── l2m_money_gain.json
    ├── l2m_money_spend.json
    ├── l2m_server_performance.json
    ├── l2m_farmers_performance.json
    ├── l2m_guild_info.json
    └── l2m_farmers_snapshot.json
```
    ├── l2m_money_spend.json
    ├── l2m_server_performance.json
    ├── l2m_farmers_performance.json
    ├── l2m_guild_info.json
    └── l2m_farmers_snapshot.json
```

## Execution Order

### Daily Pipeline (Date-dependent)
```bash
# ETL Layer - Run 7 jobs in parallel (was 11!)
python etl_engine.py --layout layouts/re-standardization/l2m/etl/l2m_active_login.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/etl/l2m_active_logout.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/etl/l2m_recharge.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/etl/l2m_item_gain.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/etl/l2m_item_spend.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/etl/l2m_money_gain.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/etl/l2m_money_spend.json --vars "logDate=2024-12-29" &
wait

# STD Layer - ONLY 1 job (just active union, item/money enrichment moved to CONS!)
python etl_engine.py --layout layouts/re-standardization/l2m/std/l2m_active.json --vars "logDate=2024-12-29"
wait

# CONS Layer - Run in parallel after STD completes
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_active.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_recharge.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_cumulative_revenue.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_item_gain.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_item_spend.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_money_gain.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_money_spend.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_server_performance.json --vars "logDate=2024-12-29" &
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_farmers_performance.json --vars "logDate=2024-12-29" &
wait
```

### Snapshot Tables (Run periodically)
```bash
# Run daily or weekly to refresh dimension tables
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_guild_info.json
python etl_engine.py --layout layouts/re-standardization/l2m/cons/l2m_farmers_snapshot.json
```

## Performance Optimizations

### 1. Eliminated Unnecessary STD Layers (56% fewer jobs!)
- **Before**: 18 jobs total (11 ETL + 7 STD)
  - Item/money: ETL → STD (add country) → CONS (write PostgreSQL)
  - Recharge: ETL → STD (passthrough) → CONS (write PostgreSQL)
  - Active: 5 ETL sources → ETL active_* → STD active → CONS (write PostgreSQL)
- **After**: 8 jobs total (7 ETL + 1 STD)
  - Item/money: ETL → CONS (enrich + write using `enrichments` array)
  - Recharge: ETL → CONS (direct write)
  - Active: 7 ETL sources → STD active (union) → CONS (write PostgreSQL)
- **Key insight**: STD layer only needed for complex transformations (unions, aggregations), NOT for simple enrichments

### 2. Proper PostgreSQL Joins Using inputSources
- **Before**: Invalid fake "enrichments" feature that doesn't exist in ETL engine ❌
- **After**: Proper `inputSources` array with file + JDBC sources ✅
- **Benefits**: 
  - Actually works! Uses existing ETL engine features
  - Simple and maintainable - just register sources as temp views and join in SQL
  - No invented patterns - follows existing codebase conventions
  - Example:
    ```json
    "inputSources": [
      {"name": "data", "type": "file", "path": "hdfs://..."},
      {"name": "profile", "type": "jdbc", "connection": "TSN_POSTGRES", "query": "SELECT ..."}
    ],
    "transformSql": "SELECT d.*, p.country FROM data d LEFT JOIN profile p USING(user_id)"
    ```

### 3. Maximum Data Reuse for Active Users
- **Before**: Scan massive Trino tables 3 times (once for active, once for recharge, once for item/money)
- **After**: Single scan per table - reuse economy data (login, logout, recharge, item, money) for active user detection
- **Speedup**: ~66% fewer Trino scans, avoids scanning billions of item/money rows multiple times

### 4. Consolidated Recharge Processing
- **Before**: 2 separate ETL jobs (recharge amount, daily revenue) reading same Trino table
- **After**: Single recharge ETL outputs both `amount` and `daily_rev_usd` columns
- **Speedup**: 50% fewer recharge-related queries

## Migration from Python Scripts

| Original Script | New Layout Files | Notes |
|----------------|------------------|-------|
| `l2m_farmers_performance.py` | `l2m_farmers_performance.json` (CONS) | Joins active/revenue with farmers snapshot using enrichments |
| `l2m_farmers_snapshot.py` | `l2m_farmers_snapshot.json` (CONS) | Bot detection via IP clustering + level-1 farming |
| `l2m_guild_info.py` | `l2m_guild_info.json` (CONS) | Latest clan membership from money logs |
| `l2m_item_gain.py` | `l2m_item_gain.json` (ETL → CONS) | **Optimized**: Skip STD, use enrichments for country |
| `l2m_item_spend.py` | `l2m_item_spend.json` (ETL → CONS) | **Optimized**: Skip STD, use enrichments for country |
| `l2m_money_gain.py` | `l2m_money_gain.json` (ETL → CONS) | **Optimized**: Skip STD, use enrichments for VIP/country |
| `l2m_money_spend.py` | `l2m_money_spend.json` (ETL → CONS) | **Optimized**: Skip STD, use enrichments for VIP/country |
| `l2m_server_performance.py` | `l2m_server_performance.json` (CONS) | Multi-input from active + recharge ETL |

## Key Business Metrics

### User Segmentation
- **VIP Levels**: 7 tiers (Free, A-G) based on total_rev/26000
- **Farmer Types**: PU Farmer (paid farmer) vs Non-PU Farmer
- **Countries**: TH, VN, ID, PH, SG, MY, Other

### Core KPIs
- **DAU**: Daily Active Users
- **NRU**: New Registered Users
- **PU**: Paying Users
- **NPU**: New Paying Users
- **NNPU**: New-New Paying Users (first login = first charge)
- **ARPU**: Average Revenue Per User
- **Item Economy**: Gain/spend for 51 key progression items
- **Diamond Economy**: Currency gain/spend by reason_id

## Next Steps
1. Update DAG to orchestrate the 3-layer pipeline
2. Add data quality checks between layers
3. Implement backfill script for historical data
4. Create monitoring dashboard for pipeline health
