# Re-Standardization: Adding a New Game - Complete Guide

**Purpose:** Step-by-step guide to migrate a new game's historical data from legacy GDS schemas to the standardized TSN Postgres format.

---

## 📋 Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [The Three-Stage Pipeline](#the-three-stage-pipeline)
4. [Step-by-Step: Add a New Game](#step-by-step-add-a-new-game)
5. [Validation & Testing](#validation--testing)
6. [DAG Configuration](#dag-configuration)
7. [Troubleshooting](#troubleshooting)
8. [Reference](#reference)

---

## Overview

### What is Re-Standardization?

**Re-standardization** migrates historical game data from:
- **Legacy:** Game-specific schemas in GDS (Trino/Postgres)
- **To:** Unified standardized schema in TSN Postgres

**Why?**
- ✅ Consistent data format across all games
- ✅ Unified reporting and analytics
- ✅ Historical data preservation
- ✅ Support for new features (VIP tiers, cumulative metrics, etc.)

### Current Status

| Game | Status | Tables Migrated |
|------|--------|----------------|
| **l2m** | ✅ Complete | recharge, active, daily_revenue, cumulative_revenue, server_performance |
| **cft** | 📝 Example below | - |
| **cs** | 🔄 Planned | - |

---

## Prerequisites

### 1. Knowledge Required

Before starting, you should understand:
- [ ] Layout file structure (read [WIKI_LAYOUT_GUIDE.md](WIKI_LAYOUT_GUIDE.md))
- [ ] SQL templates and Jinja2 macros (read [WIKI_SQL_TEMPLATE_GUIDE.md](WIKI_SQL_TEMPLATE_GUIDE.md))
- [ ] Basic Spark SQL and PySpark
- [ ] Your game's database schema

### 2. Tools & Access

- [ ] Access to GDS (game database) - Trino and/or Postgres
- [ ] Access to TSN Postgres (analytics database)
- [ ] SSH access to Airflow server
- [ ] Git repository access
- [ ] Sample data for testing (at least 1 day)

### 3. Information Gathering

For your game, document:

| Information | Example | Your Game |
|-------------|---------|-----------|
| Game ID | `cft` | __________ |
| Database type | Trino / Postgres / Both | __________ |
| Schema name | `iceberg.cft` | __________ |
| Recharge table | `iceberg.cft.etl_recharge` | __________ |
| Active table | `iceberg.cft.etl_active` | __________ |
| Date column | `ds` / `log_date` | __________ |
| User ID column | `user_id` / `role_id` | __________ |
| Currency column | `currency_type` / `currency_code` | __________ |
| Sample date with data | `2024-12-25` | __________ |

---

## The Three-Stage Pipeline

Re-standardization uses a **three-stage architecture**:

```
┌─────────────────────────────────────────────┐
│  GDS (Legacy Data)                          │
│  - Trino: iceberg.{game}.etl_*              │
│  - Postgres: {game}.raw_*                   │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  STAGE 1: ETL (Extract & Transform)         │
│  Purpose: Extract from GDS, light cleanup   │
│  Input: GDS Trino/Postgres tables           │
│  Output: HDFS Parquet                       │
│  Location: hdfs://.../etl/{table}/{date}    │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  STAGE 2: STD (Standardization)             │
│  Purpose: Apply standardized schema         │
│  Input: HDFS Parquet from Stage 1           │
│  Output: TSN Postgres                       │
│  Location: tsn.public.std_{game}_{table}    │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  STAGE 3: CONS (Consolidation)              │
│  Purpose: Complex joins & calculations      │
│  Input: HDFS + TSN Postgres                 │
│  Output: TSN Postgres                       │
│  Location: tsn.public.cons_{game}_{table}   │
└─────────────────────────────────────────────┘
```

### Stage Descriptions

#### Stage 1: ETL (Extract & Transform)

**Purpose:** Extract data from legacy GDS and save as intermediate Parquet files.

**Tasks:**
- Connect to GDS (Trino/Postgres)
- Extract data for specific date
- Light transformations (column mapping, type casting)
- Filter invalid data (nulls, negatives)
- Save to HDFS as Parquet

**Layouts:** `layouts/re-standardization/l2m/etl/`

#### Stage 2: STD (Standardization)

**Purpose:** Load Parquet data and write to standardized Postgres tables.

**Tasks:**
- Read Parquet from Stage 1
- Apply standardized schema
- Add metadata columns (`game_id`, `ds`, `created_at`)
- Write to TSN Postgres staging tables
- Partition by date for incremental loads

**Layouts:** `layouts/re-standardization/l2m/std/`

#### Stage 3: CONS (Consolidation)

**Purpose:** Complex aggregations and multi-source joins.

**Tasks:**
- Join multiple sources (ETL Parquet + STD Postgres)
- Calculate cumulative metrics (revenue, retention)
- Apply business logic (VIP tiers, revenue forecasts)
- Write to final consolidated tables

**Layouts:** `layouts/re-standardization/l2m/cons/`

---

## Step-by-Step: Add a New Game

**Example:** We'll add **Cafe Town (cft)** to re-standardization.

### Timeline

- **Investigation:** 2 hours
- **Stage 1 (ETL):** 4 hours
- **Stage 2 (STD):** 3 hours
- **Stage 3 (CONS):** 4 hours
- **Testing:** 2 hours
- **DAG Setup:** 1 hour

**Total:** ~16 hours (2 days)

---

### STEP 1: Investigate Legacy Data

**Goal:** Understand the game's database structure.

#### 1.1 Connect to GDS

```bash
# SSH to server with database access
ssh user@gds-server

# Connect to Trino
trino --server trino.example.com:8080 --catalog iceberg

# Or Postgres
psql -h postgres-server -U username -d gds_db
```

#### 1.2 Explore Schema

**Trino:**
```sql
-- Find game schemas
SHOW SCHEMAS LIKE '%cft%';

-- List tables
SHOW TABLES FROM iceberg.cft;

-- Examine structure
DESCRIBE iceberg.cft.etl_recharge;
DESCRIBE iceberg.cft.etl_active;

-- Sample data
SELECT * FROM iceberg.cft.etl_recharge WHERE ds = '2024-12-25' LIMIT 5;
SELECT * FROM iceberg.cft.etl_active WHERE ds = '2024-12-25' LIMIT 5;
```

**Postgres:**
```sql
-- List tables
\dt *cft*

-- Examine structure
\d cft.recharge
\d cft.active_users

-- Sample data
SELECT * FROM cft.recharge WHERE log_date = '2024-12-25' LIMIT 5;
SELECT * FROM cft.active_users WHERE log_date = '2024-12-25' LIMIT 5;
```

#### 1.3 Document Schema

Create a schema document:

**File:** `docs/games/cft_schema.md`

```markdown
# CFT Database Schema

## Source: GDS Trino
- Schema: `iceberg.cft`
- Date column: `ds` (format: YYYY-MM-DD)

## Recharge Table: `iceberg.cft.etl_recharge`

| Column | Type | Description | Sample Value |
|--------|------|-------------|--------------|
| user_id | BIGINT | Unique user identifier | 123456789 |
| server_id | INT | Server/zone ID | 5 |
| recharge_time | TIMESTAMP | Payment timestamp | 2024-12-25 14:30:00 |
| amount | DECIMAL(10,2) | Payment amount | 99.00 |
| currency_type | STRING | Currency code | THB |
| order_id | STRING | Transaction ID | ORD_20241225_123 |
| channel | STRING | Payment channel | google_play |
| ds | STRING | Partition date | 2024-12-25 |

## Active Table: `iceberg.cft.etl_active`

| Column | Type | Description | Sample Value |
|--------|------|-------------|--------------|
| user_id | BIGINT | Unique user identifier | 123456789 |
| server_id | INT | Server/zone ID | 5 |
| login_time | TIMESTAMP | First login of day | 2024-12-25 08:00:00 |
| logout_time | TIMESTAMP | Last logout of day | 2024-12-25 23:45:00 |
| online_duration | INT | Total seconds online | 18000 |
| level | INT | Character level | 45 |
| ds | STRING | Partition date | 2024-12-25 |

## Data Quality Notes
- Missing values: ~0.1% of user_id are NULL
- Currency: All payments in THB (no conversion needed)
- Servers: 10 servers (IDs 1-10)
- Date range: 2023-01-01 to present
```

---

### STEP 2: Create Stage 1 (ETL) - Extract from GDS

**Goal:** Extract data from GDS and save as Parquet.

#### 2.1 Create SQL Extraction Queries

**File:** `templates/re-standardization/cft/recharge.sql.j2`

```jinja
{% import 'sql/macros.sql' as macros %}

-- CFT Recharge ETL: Extract from GDS Trino
-- Filters invalid data and standardizes column names

SELECT 
    user_id,
    server_id,
    recharge_time,
    amount,
    currency_type AS currency_code,
    order_id,
    channel AS payment_channel,
    ds
FROM iceberg.cft.etl_recharge
WHERE {{ macros.date_filter('ds', log_date) }}
    AND user_id IS NOT NULL
    AND amount > 0
    AND currency_type IS NOT NULL
```

**File:** `templates/re-standardization/cft/active.sql.j2`

```jinja
{% import 'sql/macros.sql' as macros %}

-- CFT Active Users ETL: Extract from GDS Trino
-- Calculates daily active users with session metrics

SELECT 
    user_id,
    server_id,
    login_time AS first_login_time,
    logout_time AS last_logout_time,
    online_duration AS session_duration_seconds,
    level AS character_level,
    ds
FROM iceberg.cft.etl_active
WHERE {{ macros.date_filter('ds', log_date) }}
    AND user_id IS NOT NULL
    AND online_duration > 0
```

#### 2.2 Create ETL Layout Files

**File:** `layouts/re-standardization/l2m/etl/cft_recharge.json`

```json
{
  "description": "CFT Recharge ETL: Extract recharge data from GDS",
  "comment": "Stage 1: Extract raw recharge transactions from Trino, filter invalid data",
  
  "inputSources": [
    {
      "name": "raw_recharge",
      "type": "jdbc",
      "connection": "GDS_TRINO",
      "table": "iceberg.cft.etl_recharge"
    }
  ],
  
  "sqlFile": "templates/re-standardization/cft/recharge.sql.j2",
  
  "outputs": [{
    "type": "file",
    "path": "hdfs://c0s/user/gsbkk-workspace-yc9t6/re-standardization/cft/etl/recharge/{logDate}",
    "format": "parquet",
    "mode": "overwrite",
    "numPartitions": 4
  }]
}
```

**File:** `layouts/re-standardization/l2m/etl/cft_active.json`

```json
{
  "description": "CFT Active ETL: Extract active user data from GDS",
  "comment": "Stage 1: Extract daily active users from Trino, calculate session metrics",
  
  "inputSources": [
    {
      "name": "raw_active",
      "type": "jdbc",
      "connection": "GDS_TRINO",
      "table": "iceberg.cft.etl_active"
    }
  ],
  
  "sqlFile": "templates/re-standardization/cft/active.sql.j2",
  
  "outputs": [{
    "type": "file",
    "path": "hdfs://c0s/user/gsbkk-workspace-yc9t6/re-standardization/cft/etl/active/{logDate}",
    "format": "parquet",
    "mode": "overwrite",
    "numPartitions": 4
  }]
}
```

#### 2.3 Test Stage 1

**Test with sample date:**

```bash
cd /opt/airflow/dags/repo/gsbkk

# Test recharge extraction
./run_etl_process.sh \
  --layout layouts/re-standardization/l2m/etl/cft_recharge.json \
  --vars "gameId=cft,logDate=2024-12-25"

# Test active extraction
./run_etl_process.sh \
  --layout layouts/re-standardization/l2m/etl/cft_active.json \
  --vars "gameId=cft,logDate=2024-12-25"
```

**Verify output:**

```bash
# Check Parquet files created
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/re-standardization/cft/etl/recharge/2024-12-25/
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/re-standardization/cft/etl/active/2024-12-25/

# Count records
spark-sql -e "SELECT COUNT(*) FROM parquet.\`hdfs://c0s/.../cft/etl/recharge/2024-12-25\`"
spark-sql -e "SELECT COUNT(*) FROM parquet.\`hdfs://c0s/.../cft/etl/active/2024-12-25\`"
```

---

### STEP 3: Create Stage 2 (STD) - Standardize to Postgres

**Goal:** Load Parquet and write to standardized Postgres tables.

#### 3.1 Define Standard Schema

**File:** `docs/games/cft_standard_schema.md`

```markdown
# CFT Standardized Schema

## Table: `tsn_postgres.public.std_cft_recharge`

Standardized recharge transactions with metadata columns.

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| user_id | BIGINT | User identifier | etl.user_id |
| server_id | INT | Server ID | etl.server_id |
| transaction_time | TIMESTAMP | Payment timestamp | etl.recharge_time |
| amount | DECIMAL(10,2) | Payment amount | etl.amount |
| currency_code | VARCHAR(10) | Currency | etl.currency_code |
| order_id | VARCHAR(100) | Transaction ID | etl.order_id |
| payment_channel | VARCHAR(50) | Payment method | etl.payment_channel |
| **game_id** | VARCHAR(10) | Game identifier | **'cft' (added)** |
| **ds** | DATE | Partition date | **{logDate} (added)** |
| **created_at** | TIMESTAMP | ETL timestamp | **NOW() (added)** |

## Table: `tsn_postgres.public.std_cft_active`

Standardized daily active users with metadata.

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| user_id | BIGINT | User identifier | etl.user_id |
| server_id | INT | Server ID | etl.server_id |
| first_login_time | TIMESTAMP | First login | etl.first_login_time |
| last_logout_time | TIMESTAMP | Last logout | etl.last_logout_time |
| session_duration_seconds | INT | Online time | etl.session_duration_seconds |
| character_level | INT | Character level | etl.character_level |
| **game_id** | VARCHAR(10) | Game identifier | **'cft' (added)** |
| **ds** | DATE | Partition date | **{logDate} (added)** |
| **created_at** | TIMESTAMP | ETL timestamp | **NOW() (added)** |
```

#### 3.2 Create SQL Standardization Queries

**File:** `templates/re-standardization/cft/recharge_std.sql.j2`

```jinja
-- CFT Recharge Standardization
-- Adds metadata columns (game_id, ds, created_at)

SELECT 
    user_id,
    server_id,
    recharge_time AS transaction_time,
    amount,
    currency_code,
    order_id,
    payment_channel,
    
    -- Metadata columns
    '{{ game_id }}' AS game_id,
    DATE '{{ log_date }}' AS ds,
    CURRENT_TIMESTAMP AS created_at
    
FROM etl_recharge
```

**File:** `templates/re-standardization/cft/active_std.sql.j2`

```jinja
-- CFT Active Standardization
-- Adds metadata columns

SELECT 
    user_id,
    server_id,
    first_login_time,
    last_logout_time,
    session_duration_seconds,
    character_level,
    
    -- Metadata columns
    '{{ game_id }}' AS game_id,
    DATE '{{ log_date }}' AS ds,
    CURRENT_TIMESTAMP AS created_at
    
FROM etl_active
```

#### 3.3 Create STD Layout Files

**File:** `layouts/re-standardization/l2m/std/cft_recharge.json`

```json
{
  "description": "CFT Recharge STD: Standardize and load to Postgres",
  "comment": "Stage 2: Load Parquet from ETL, add metadata, write to TSN Postgres",
  
  "inputSources": [
    {
      "name": "etl_recharge",
      "type": "file",
      "path": "hdfs://c0s/user/gsbkk-workspace-yc9t6/re-standardization/cft/etl/recharge/{logDate}",
      "format": "parquet"
    }
  ],
  
  "sqlFile": "templates/re-standardization/cft/recharge_std.sql.j2",
  
  "outputs": [{
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.std_cft_recharge",
    "mode": "overwrite",
    "deleteCondition": "ds = '{logDate}'"
  }]
}
```

**File:** `layouts/re-standardization/l2m/std/cft_active.json`

```json
{
  "description": "CFT Active STD: Standardize and load to Postgres",
  "comment": "Stage 2: Load Parquet from ETL, add metadata, write to TSN Postgres",
  
  "inputSources": [
    {
      "name": "etl_active",
      "type": "file",
      "path": "hdfs://c0s/user/gsbkk-workspace-yc9t6/re-standardization/cft/etl/active/{logDate}",
      "format": "parquet"
    }
  ],
  
  "sqlFile": "templates/re-standardization/cft/active_std.sql.j2",
  
  "outputs": [{
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.std_cft_active",
    "mode": "overwrite",
    "deleteCondition": "ds = '{logDate}'"
  }]
}
```

#### 3.4 Create Postgres Tables

**File:** `sql/ddl/std_cft_tables.sql`

```sql
-- Create standardized tables for CFT
-- Run this before first STD load

-- Recharge table
CREATE TABLE IF NOT EXISTS public.std_cft_recharge (
    user_id BIGINT NOT NULL,
    server_id INT,
    transaction_time TIMESTAMP,
    amount DECIMAL(10,2),
    currency_code VARCHAR(10),
    order_id VARCHAR(100),
    payment_channel VARCHAR(50),
    game_id VARCHAR(10) NOT NULL,
    ds DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (user_id, order_id, ds)
);

CREATE INDEX idx_std_cft_recharge_ds ON public.std_cft_recharge(ds);
CREATE INDEX idx_std_cft_recharge_user ON public.std_cft_recharge(user_id);

-- Active users table
CREATE TABLE IF NOT EXISTS public.std_cft_active (
    user_id BIGINT NOT NULL,
    server_id INT,
    first_login_time TIMESTAMP,
    last_logout_time TIMESTAMP,
    session_duration_seconds INT,
    character_level INT,
    game_id VARCHAR(10) NOT NULL,
    ds DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (user_id, ds)
);

CREATE INDEX idx_std_cft_active_ds ON public.std_cft_active(ds);
CREATE INDEX idx_std_cft_active_user ON public.std_cft_active(user_id);
```

**Execute DDL:**

```bash
psql -h tsn-postgres-server -U username -d analytics -f sql/ddl/std_cft_tables.sql
```

#### 3.5 Test Stage 2

```bash
# Test standardization
./run_etl_process.sh \
  --layout layouts/re-standardization/l2m/std/cft_recharge.json \
  --vars "gameId=cft,logDate=2024-12-25"

./run_etl_process.sh \
  --layout layouts/re-standardization/l2m/std/cft_active.json \
  --vars "gameId=cft,logDate=2024-12-25"
```

**Verify in Postgres:**

```sql
-- Check data loaded
SELECT COUNT(*) FROM public.std_cft_recharge WHERE ds = '2024-12-25';
SELECT COUNT(*) FROM public.std_cft_active WHERE ds = '2024-12-25';

-- Verify metadata columns
SELECT game_id, ds, created_at 
FROM public.std_cft_recharge 
WHERE ds = '2024-12-25' 
LIMIT 5;

-- Check for nulls
SELECT 
    COUNT(*) FILTER (WHERE user_id IS NULL) AS null_users,
    COUNT(*) FILTER (WHERE amount IS NULL) AS null_amounts,
    COUNT(*) FILTER (WHERE ds IS NULL) AS null_dates
FROM public.std_cft_recharge
WHERE ds = '2024-12-25';
```

---

### STEP 4: Create Stage 3 (CONS) - Consolidation

**Goal:** Join data sources and calculate complex metrics.

#### 4.1 Define Consolidated Tables

**Example:** Daily revenue summary with server performance

**File:** `templates/re-standardization/cft/daily_revenue_cons.sql.j2`

```jinja
{% import 'sql/macros.sql' as macros %}

-- CFT Daily Revenue Consolidation
-- Joins recharge and active data, calculates metrics

WITH daily_recharge AS (
    SELECT 
        ds,
        server_id,
        COUNT(DISTINCT user_id) AS paying_users,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_revenue_per_user,
        MAX(amount) AS max_payment
    FROM std_cft_recharge
    WHERE ds = DATE '{{ log_date }}'
    GROUP BY ds, server_id
),

daily_active AS (
    SELECT 
        ds,
        server_id,
        COUNT(DISTINCT user_id) AS active_users,
        AVG(session_duration_seconds) AS avg_session_seconds,
        AVG(character_level) AS avg_character_level
    FROM std_cft_active
    WHERE ds = DATE '{{ log_date }}'
    GROUP BY ds, server_id
)

SELECT 
    r.ds,
    r.server_id,
    COALESCE(a.active_users, 0) AS active_users,
    COALESCE(r.paying_users, 0) AS paying_users,
    COALESCE(r.total_revenue, 0) AS total_revenue,
    COALESCE(r.avg_revenue_per_user, 0) AS arpu,
    COALESCE(r.max_payment, 0) AS max_payment,
    COALESCE(a.avg_session_seconds, 0) AS avg_session_seconds,
    COALESCE(a.avg_character_level, 0) AS avg_character_level,
    
    -- Calculated metrics
    CASE 
        WHEN a.active_users > 0 
        THEN CAST(r.paying_users AS DOUBLE) / a.active_users * 100
        ELSE 0 
    END AS conversion_rate_pct,
    
    '{{ game_id }}' AS game_id,
    CURRENT_TIMESTAMP AS created_at
    
FROM daily_recharge r
FULL OUTER JOIN daily_active a 
    ON r.ds = a.ds 
    AND r.server_id = a.server_id
```

#### 4.2 Create CONS Layout

**File:** `layouts/re-standardization/l2m/cons/cft_daily_revenue.json`

```json
{
  "description": "CFT Daily Revenue CONS: Consolidated daily metrics",
  "comment": "Stage 3: Join recharge and active data, calculate KPIs",
  
  "inputSources": [
    {
      "name": "std_cft_recharge",
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "table": "(SELECT * FROM public.std_cft_recharge WHERE ds = '{logDate}') AS std_cft_recharge"
    },
    {
      "name": "std_cft_active",
      "type": "jdbc",
      "connection": "TSN_POSTGRES",
      "table": "(SELECT * FROM public.std_cft_active WHERE ds = '{logDate}') AS std_cft_active"
    }
  ],
  
  "sqlFile": "templates/re-standardization/cft/daily_revenue_cons.sql.j2",
  
  "outputs": [{
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.cons_cft_daily_revenue",
    "mode": "overwrite",
    "deleteCondition": "ds = '{logDate}'"
  }]
}
```

#### 4.3 Create CONS Table

**File:** `sql/ddl/cons_cft_tables.sql`

```sql
-- Create consolidated table for CFT

CREATE TABLE IF NOT EXISTS public.cons_cft_daily_revenue (
    ds DATE NOT NULL,
    server_id INT NOT NULL,
    active_users INT,
    paying_users INT,
    total_revenue DECIMAL(12,2),
    arpu DECIMAL(10,2),
    max_payment DECIMAL(10,2),
    avg_session_seconds INT,
    avg_character_level DECIMAL(5,2),
    conversion_rate_pct DECIMAL(5,2),
    game_id VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (ds, server_id)
);

CREATE INDEX idx_cons_cft_daily_ds ON public.cons_cft_daily_revenue(ds);
```

```bash
psql -h tsn-postgres-server -U username -d analytics -f sql/ddl/cons_cft_tables.sql
```

#### 4.4 Test Stage 3

```bash
./run_etl_process.sh \
  --layout layouts/re-standardization/l2m/cons/cft_daily_revenue.json \
  --vars "gameId=cft,logDate=2024-12-25"
```

**Verify:**

```sql
SELECT * FROM public.cons_cft_daily_revenue WHERE ds = '2024-12-25' ORDER BY server_id;

-- Check metrics make sense
SELECT 
    ds,
    SUM(active_users) AS total_active,
    SUM(paying_users) AS total_paying,
    SUM(total_revenue) AS total_rev,
    AVG(conversion_rate_pct) AS avg_conversion
FROM public.cons_cft_daily_revenue
WHERE ds = '2024-12-25'
GROUP BY ds;
```

---

### STEP 5: Create Directory Structure

**Organize files properly:**

```bash
cd /opt/airflow/dags/repo/gsbkk

# Create directories
mkdir -p layouts/re-standardization/l2m/etl
mkdir -p layouts/re-standardization/l2m/std
mkdir -p layouts/re-standardization/l2m/cons
mkdir -p templates/re-standardization/cft
mkdir -p docs/games
mkdir -p sql/ddl

# Move files to correct locations
mv cft_*.json layouts/re-standardization/l2m/etl/
mv cft_*_std.json layouts/re-standardization/l2m/std/
mv cft_*_cons.json layouts/re-standardization/l2m/cons/
mv *.sql.j2 templates/re-standardization/cft/
```

**Final structure:**

```
gsbkk/
├── layouts/
│   └── re-standardization/
│       └── l2m/
│           ├── etl/
│           │   ├── cft_recharge.json
│           │   └── cft_active.json
│           ├── std/
│           │   ├── cft_recharge.json
│           │   └── cft_active.json
│           └── cons/
│               └── cft_daily_revenue.json
├── templates/
│   └── re-standardization/
│       └── cft/
│           ├── recharge.sql.j2
│           ├── active.sql.j2
│           ├── recharge_std.sql.j2
│           ├── active_std.sql.j2
│           └── daily_revenue_cons.sql.j2
├── sql/
│   └── ddl/
│       ├── std_cft_tables.sql
│       └── cons_cft_tables.sql
└── docs/
    └── games/
        ├── cft_schema.md
        └── cft_standard_schema.md
```

---

## Validation & Testing

### Test Checklist

Before deploying to production:

- [ ] **Stage 1 (ETL):**
  - [ ] Data extracted from GDS successfully
  - [ ] Parquet files created in HDFS
  - [ ] Record count matches source
  - [ ] No null values in critical columns
  - [ ] Date partition working correctly

- [ ] **Stage 2 (STD):**
  - [ ] Data loaded to Postgres tables
  - [ ] Metadata columns (game_id, ds, created_at) populated
  - [ ] Primary keys and indexes created
  - [ ] No duplicate records
  - [ ] Data types match schema

- [ ] **Stage 3 (CONS):**
  - [ ] Multi-source joins working
  - [ ] Calculated metrics correct (spot check)
  - [ ] No unexpected nulls
  - [ ] Aggregations sum correctly

- [ ] **End-to-End:**
  - [ ] Run full pipeline (ETL → STD → CONS) for 1 day
  - [ ] Run for 3 consecutive days (test incremental)
  - [ ] Compare results with legacy reports
  - [ ] Performance acceptable (<15 minutes per day)

### Testing Commands

**Full pipeline test:**

```bash
# Set variables
GAME_ID="cft"
LOG_DATE="2024-12-25"

# Stage 1: ETL
./run_etl_process.sh --layout layouts/re-standardization/l2m/etl/cft_recharge.json --vars "gameId=$GAME_ID,logDate=$LOG_DATE"
./run_etl_process.sh --layout layouts/re-standardization/l2m/etl/cft_active.json --vars "gameId=$GAME_ID,logDate=$LOG_DATE"

# Stage 2: STD
./run_etl_process.sh --layout layouts/re-standardization/l2m/std/cft_recharge.json --vars "gameId=$GAME_ID,logDate=$LOG_DATE"
./run_etl_process.sh --layout layouts/re-standardization/l2m/std/cft_active.json --vars "gameId=$GAME_ID,logDate=$LOG_DATE"

# Stage 3: CONS
./run_etl_process.sh --layout layouts/re-standardization/l2m/cons/cft_daily_revenue.json --vars "gameId=$GAME_ID,logDate=$LOG_DATE"

# Verify final output
psql -h tsn-postgres-server -U username -d analytics -c "SELECT * FROM public.cons_cft_daily_revenue WHERE ds = '$LOG_DATE';"
```

**Date range test:**

```bash
# Test multiple days
for DATE in 2024-12-23 2024-12-24 2024-12-25; do
    echo "Processing $DATE..."
    ./run_etl_process.sh --layout layouts/re-standardization/l2m/etl/cft_recharge.json --vars "gameId=cft,logDate=$DATE"
    ./run_etl_process.sh --layout layouts/re-standardization/l2m/etl/cft_active.json --vars "gameId=cft,logDate=$DATE"
    ./run_etl_process.sh --layout layouts/re-standardization/l2m/std/cft_recharge.json --vars "gameId=cft,logDate=$DATE"
    ./run_etl_process.sh --layout layouts/re-standardization/l2m/std/cft_active.json --vars "gameId=cft,logDate=$DATE"
    ./run_etl_process.sh --layout layouts/re-standardization/l2m/cons/cft_daily_revenue.json --vars "gameId=cft,logDate=$DATE"
done
```

---

## DAG Configuration

### Create Airflow DAG

**File:** `dags/re_standardization_cft_dag.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email': ['data-eng@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    're_standardization_cft',
    default_args=default_args,
    description='CFT Re-standardization: ETL → STD → CONS',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['re-standardization', 'cft'],
) as dag:

    # Stage 1: ETL
    etl_recharge = BashOperator(
        task_id='etl_recharge',
        bash_command=(
            'cd /opt/airflow/dags/repo/gsbkk && '
            './run_etl_process.sh '
            '--layout layouts/re-standardization/l2m/etl/cft_recharge.json '
            '--vars "gameId=cft,logDate={{ ds }}"'
        ),
    )

    etl_active = BashOperator(
        task_id='etl_active',
        bash_command=(
            'cd /opt/airflow/dags/repo/gsbkk && '
            './run_etl_process.sh '
            '--layout layouts/re-standardization/l2m/etl/cft_active.json '
            '--vars "gameId=cft,logDate={{ ds }}"'
        ),
    )

    # Stage 2: STD
    std_recharge = BashOperator(
        task_id='std_recharge',
        bash_command=(
            'cd /opt/airflow/dags/repo/gsbkk && '
            './run_etl_process.sh '
            '--layout layouts/re-standardization/l2m/std/cft_recharge.json '
            '--vars "gameId=cft,logDate={{ ds }}"'
        ),
    )

    std_active = BashOperator(
        task_id='std_active',
        bash_command=(
            'cd /opt/airflow/dags/repo/gsbkk && '
            './run_etl_process.sh '
            '--layout layouts/re-standardization/l2m/std/cft_active.json '
            '--vars "gameId=cft,logDate={{ ds }}"'
        ),
    )

    # Stage 3: CONS
    cons_daily_revenue = BashOperator(
        task_id='cons_daily_revenue',
        bash_command=(
            'cd /opt/airflow/dags/repo/gsbkk && '
            './run_etl_process.sh '
            '--layout layouts/re-standardization/l2m/cons/cft_daily_revenue.json '
            '--vars "gameId=cft,logDate={{ ds }}"'
        ),
    )

    # Dependencies
    [etl_recharge, etl_active] >> [std_recharge, std_active] >> cons_daily_revenue
```

**Deploy DAG:**

```bash
# Copy to Airflow DAGs folder
cp dags/re_standardization_cft_dag.py /opt/airflow/dags/

# Verify DAG
airflow dags list | grep cft

# Test DAG
airflow dags test re_standardization_cft 2024-12-25

# Trigger manually
airflow dags trigger re_standardization_cft --conf '{"logDate": "2024-12-25"}'
```

---

## Troubleshooting

### Common Issues

#### 1. JDBC Connection Fails

**Error:**
```
Connection refused: GDS_POSTGRES
```

**Fix:**
- Verify connection in `configs/data_path.yaml`
- Test connection manually:
  ```bash
  psql -h gds-postgres-host -U username -d database
  ```
- Check firewall rules

#### 2. Parquet Files Not Created

**Error:**
```
Path not found: hdfs://c0s/.../etl/recharge/2024-12-25
```

**Fix:**
- Check HDFS permissions:
  ```bash
  hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/re-standardization/cft/etl/
  ```
- Verify Spark can write to HDFS
- Check path in layout matches actual structure

#### 3. Table Already Exists Error

**Error:**
```
ERROR: duplicate key value violates unique constraint "std_cft_recharge_pkey"
```

**Fix:**
- Use `deleteCondition` in output:
  ```json
  {
    "type": "jdbc",
    "table": "public.std_cft_recharge",
    "mode": "overwrite",
    "deleteCondition": "ds = '{logDate}'"
  }
  ```
- Or manually delete before running:
  ```sql
  DELETE FROM public.std_cft_recharge WHERE ds = '2024-12-25';
  ```

#### 4. SQL Template Not Found

**Error:**
```
jinja2.exceptions.TemplateNotFound: cft/recharge.sql.j2
```

**Fix:**
- Check file exists: `templates/re-standardization/cft/recharge.sql.j2`
- Verify path in layout: `"sqlFile": "templates/re-standardization/cft/recharge.sql.j2"`
- Ensure template directory is in search path

#### 5. Data Mismatch - Record Count Different

**Investigation:**

```sql
-- Count in source
SELECT COUNT(*) FROM iceberg.cft.etl_recharge WHERE ds = '2024-12-25';

-- Count in ETL Parquet
-- (check via Spark)

-- Count in STD Postgres
SELECT COUNT(*) FROM public.std_cft_recharge WHERE ds = '2024-12-25';

-- Check filters
SELECT 
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE user_id IS NULL) AS null_users,
    COUNT(*) FILTER (WHERE amount <= 0) AS invalid_amount
FROM iceberg.cft.etl_recharge 
WHERE ds = '2024-12-25';
```

**Fix:**
- Review SQL filters (WHERE clauses)
- Check for data quality issues in source
- Adjust filters if too aggressive

---

## Reference

### File Naming Conventions

| Stage | Directory | Filename Pattern | Example |
|-------|-----------|------------------|---------|
| ETL | `layouts/re-standardization/l2m/etl/` | `{game}_{table}.json` | `cft_recharge.json` |
| STD | `layouts/re-standardization/l2m/std/` | `{game}_{table}.json` | `cft_recharge.json` |
| CONS | `layouts/re-standardization/l2m/cons/` | `{game}_{metric}.json` | `cft_daily_revenue.json` |
| SQL Templates | `templates/re-standardization/{game}/` | `{table}.sql.j2` | `recharge.sql.j2` |
| DDL | `sql/ddl/` | `{stage}_{game}_tables.sql` | `std_cft_tables.sql` |

### Standard Table Names

| Stage | Pattern | Example |
|-------|---------|---------|
| ETL (HDFS) | `hdfs://.../re-standardization/{game}/etl/{table}/{date}` | `.../cft/etl/recharge/2024-12-25` |
| STD (Postgres) | `public.std_{game}_{table}` | `public.std_cft_recharge` |
| CONS (Postgres) | `public.cons_{game}_{metric}` | `public.cons_cft_daily_revenue` |

### Required Metadata Columns

All STD and CONS tables must include:

| Column | Type | Description | Value |
|--------|------|-------------|-------|
| `game_id` | VARCHAR(10) | Game identifier | `'cft'` |
| `ds` | DATE | Partition date | `'2024-12-25'` |
| `created_at` | TIMESTAMP | ETL timestamp | `CURRENT_TIMESTAMP` |

---

## Next Steps

📚 **Related Documentation:**
- [Layout Guide](WIKI_LAYOUT_GUIDE.md) - Layout file structure
- [SQL Template Guide](WIKI_SQL_TEMPLATE_GUIDE.md) - Jinja2 templates and macros
- [L2M Re-standardization](../layouts/re-standardization/README.md) - Reference implementation

🎯 **Quick Actions:**
- Copy L2M layouts as template: `cp -r layouts/re-standardization/l2m/etl/l2m_*.json layouts/re-standardization/l2m/etl/cft_*.json`
- Review existing templates: `templates/re-standardization/l2m/`
- Test with sample data: Use checklist above
- Deploy DAG: See DAG configuration section

---

**Last Updated:** 2026-01-07  
**Maintainer:** Data Engineering Team  
**Contact:** data-eng@company.com
