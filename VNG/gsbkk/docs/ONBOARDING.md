# GSBKK — New Analyst Onboarding Guide

Welcome to GSBKK. This document covers everything you need to start building and maintaining data pipelines.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Repository Layout](#2-repository-layout)
3. [The Five Pipeline Patterns](#3-the-five-pipeline-patterns)
4. [Writing SQL Templates](#4-writing-sql-templates)
5. [Writing Airflow DAGs](#5-writing-airflow-dags)
6. [Connections and Credentials](#6-connections-and-credentials)
7. [HDFS Path Conventions](#7-hdfs-path-conventions)
8. [Adding a New Pipeline](#8-adding-a-new-pipeline)
9. [Deployment](#9-deployment)
10. [Debugging](#10-debugging)
11. [Active Projects](#11-active-projects)

---

## 1. Architecture Overview

Data flows through four layers:

```
External Source
      │
      ▼
  RAW (HDFS JSON)          ← API responses, Google Sheet exports
      │
      ▼
  ETL (HDFS Parquet)       ← Type-cast, filter, rename from source tables
      │
      ▼
  STD (HDFS Parquet)       ← Aggregate / standardise into common schema
      │
      ▼
  CONS (TSN Postgres)      ← Business-facing tables, dashboards
```

Every step is driven by a Jinja2 SQL file. There are no JSON layout config files and no Python transformation logic — just SQL.

---

## 2. Repository Layout

```
gsbkk/
├── dags/                        # Airflow DAG files
│   ├── utils/dag_helpers.py     # create_sql_operator and friends
│   ├── game_health_check/       # Per-game health check DAGs
│   ├── ltv/                     # LTV feature set DAG
│   ├── rolling_forecast/        # RFC DAGs
│   └── sensortower/             # SensorTower DAG
│
├── transform/                   # ALL SQL transformation files
│   ├── etl/                     # JDBC → HDFS Parquet
│   ├── std/                     # HDFS Parquet → HDFS Parquet
│   └── cons/                    # HDFS Parquet → Postgres
│
├── templates/                   # Game-specific SQL (game_health_check, ltv)
│   ├── game_health_check/
│   └── ltv/
│
├── src/                         # Python runner scripts (Python 2.7)
│   ├── sql_runner.py            # Spark SQL pipeline runner
│   ├── api_to_raw.py            # HTTP API → HDFS JSON
│   └── gsheet_to_raw.py        # Google Sheet → HDFS JSON
│
├── run_sql.sh                   # Wrapper for sql_runner.py (Spark)
├── run_api.sh                   # Wrapper for api_to_raw.py
├── run_gsheet.sh                # Wrapper for gsheet_to_raw.py
│
├── configs/
│   └── game_configs/game_name.yaml   # Per-game metadata (countries, sheet IDs)
│
└── docs/                        # Documentation (you are here)
```

> **Key rule:** All business logic lives in `transform/` or `templates/` as `.sql.j2` files.
> DAGs only wire tasks together — they contain no SQL or transformation logic.

---

## 3. The Five Pipeline Patterns

`create_sql_operator` in `dags/utils/dag_helpers.py` is the single entry point for all task creation. It maps to five distinct patterns depending on which arguments you pass.

### Pattern A — Single HDFS input → HDFS output

Use for STD tasks that transform one HDFS file.

```python
std_task = create_sql_operator(
    dag=dag,
    task_id='std_my_metric',
    sql_file='transform/std/my_metric.sql.j2',
    input_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/my_game/etl/metric/{logDate}',
    input_view='metric',
    output_type='file',
    output_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/my_game/std/metric/{logDate}',
    log_date='{{ ds }}',
    game_id='my_game',
)
```

### Pattern B — Multi-file HDFS inputs → HDFS output

Use for STD tasks that JOIN or UNION several HDFS datasets.

```python
std_task = create_sql_operator(
    dag=dag,
    task_id='std_active',
    sql_file='transform/std/active.sql.j2',
    input_paths='login|hdfs://.../etl/login/{logDate};logout|hdfs://.../etl/logout/{logDate}',
    output_type='file',
    output_path='hdfs://.../std/active/{logDate}',
)
```

`input_paths` format: `view_name|hdfs_path` pairs separated by `;`.

### Pattern C — JDBC extraction → HDFS output

Use for ETL tasks that pull from GDS Postgres or Trino.

```python
etl_task = create_sql_operator(
    dag=dag,
    task_id='etl_active_details',
    extract_sql_file='templates/game_health_check/my_game/active_details.sql.j2',
    extract_connection='GDS_POSTGRES',    # or 'GDS_TRINO'
    output_type='file',
    output_path='hdfs://.../game_health_check/my_game/active_details/{logDate}',
    log_date='{{ ds }}',
    game_id='my_game',
)
```

The SQL in `extract_sql_file` runs on the remote database. The result is saved to HDFS as Parquet.

### Pattern D — Multi-file HDFS inputs → JDBC output (CONS)

Use for CONS tasks that read several HDFS views and write to Postgres TSN.

```python
cons_task = create_sql_operator(
    dag=dag,
    task_id='cons_diagnostic_daily',
    sql_file='templates/game_health_check/diagnostic_daily.sql.j2',
    input_paths=';'.join([
        'currency_mapping|' + HDFS_BASE + '/currency_mapping',
        'charge_details|'   + GHC + '/std/charge/{logDate}',
        'active_details|'   + GHC + '/std/active/{logDate}',
        'campaign|'         + GHC + '/campaign',
        'retention_details|'+ GHC + '/std/retention',
    ]),
    output_type='jdbc',
    output_table='public.ghc_diagnostic_daily_my_game',
    output_mode='append',
    delete_condition="report_date = DATE '{{ logDate }}'",
    log_date='{{ ds }}',
    game_id='my_game',
)
```

### Pattern E — Multi-file HDFS + secondary JDBC → JDBC output (CONS with lookup)

Use when the CONS SQL needs a live lookup from a remote database (e.g. user_profile enrichment).

```python
cons_task = create_sql_operator(
    dag=dag,
    task_id='cons_package_performance',
    sql_file='templates/game_health_check/package_performance.sql.j2',
    input_path=GHC + '/charge_details/{logDate}',
    input_view='charge_details',
    secondary_sql_file='transform/etl/ghc_user_profile_my_game.sql.j2',
    secondary_connection='GDS_POSTGRES',
    secondary_view='user_profile',
    output_type='jdbc',
    output_table='public.ghc_package_performance_my_game',
    output_mode='append',
    delete_condition="date = '{{ logDate }}'",
    num_partitions=2,
    log_date='{{ ds }}',
    game_id='my_game',
)
```

---

## 4. Writing SQL Templates

All SQL files are Jinja2 templates with extension `.sql.j2`.

### Available Variables

| Variable   | Example      | Notes                                    |
|------------|--------------|------------------------------------------|
| `logDate`  | `2026-03-17` | Primary processing date (always set)     |
| `log_date` | `2026-03-17` | Alias for `logDate`                      |
| `gameId`   | `l2m`        | Game identifier (set via `game_id=` arg) |
| `game_id`  | `l2m`        | Alias for `gameId`                       |

### Typical ETL Template (Pattern C — JDBC source)

```sql
-- transform/etl/my_game_active.sql.j2
-- Runs on GDS_POSTGRES, result saved to HDFS as Parquet
SELECT
    CAST(user_id   AS BIGINT)  AS user_id,
    CAST(server_id AS INT)     AS server_id,
    CAST(log_date  AS DATE)    AS log_date,
    CAST(level     AS INT)     AS level
FROM ops.daily_role_active_details
WHERE log_date = DATE '{{ logDate }}'
  AND game_id = 425
```

Rules for ETL SQL:
- Cast all columns to explicit types — no implicit casts downstream
- Filter to `logDate` immediately to limit data pulled
- Use the exact column names your STD/CONS templates expect

### Typical STD Template (Pattern A/B — SparkSQL)

```sql
-- transform/std/my_game_charge.sql.j2
-- Runs in Spark, reads HDFS view registered as 'charge_details'
SELECT
    log_date,
    server_id,
    SUM(amount_vnd) AS total_vnd,
    SUM(amount_usd) AS total_usd,
    COUNT(DISTINCT user_id) AS paying_users
FROM charge_details
WHERE log_date = DATE '{{ logDate }}'
GROUP BY log_date, server_id
```

### Typical CONS Template (Patterns D/E — SparkSQL)

```sql
-- transform/cons/my_game_report.sql.j2
-- Input views: charge, active, user_profile (if secondary_sql_file set)
SELECT
    a.log_date,
    a.server_id,
    a.dau,
    c.total_usd,
    u.vip_tier
FROM active a
LEFT JOIN charge c ON a.log_date = c.log_date AND a.server_id = c.server_id
LEFT JOIN user_profile u ON a.user_id = u.user_id
WHERE a.log_date = DATE '{{ logDate }}'
```

### Macros

A shared `macros.sql` file provides reusable date helpers:

```sql
{% import 'macros.sql' as macros %}

SELECT *
FROM my_table
WHERE {{ macros.date_filter("DATE(logtime + INTERVAL '7' HOUR)", logDate) }}
```

---

## 5. Writing Airflow DAGs

### Minimal DAG Template

```python
import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.dag_helpers import create_sql_operator

HDFS_BASE = 'hdfs://c0s/user/gsbkk-workspace-yc9t6'
MY_PATH   = HDFS_BASE + '/my_project/my_game'

dag = DAG(
    dag_id='my_project_my_game',
    default_args={
        'owner': 'gsbkk',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
    },
    schedule_interval='0 3 * * *',   # 3 AM UTC daily
    catchup=True,
    max_active_runs=1,
    tags=['my_project', 'my_game'],
)

etl_task = create_sql_operator(
    dag=dag,
    task_id='etl_metric',
    extract_sql_file='transform/etl/my_game_metric.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=MY_PATH + '/etl/metric/{logDate}',
    log_date='{{ ds }}',
    game_id='my_game',
)

cons_task = create_sql_operator(
    dag=dag,
    task_id='cons_report',
    sql_file='transform/cons/my_game_report.sql.j2',
    input_path=MY_PATH + '/etl/metric/{logDate}',
    input_view='metric',
    output_type='jdbc',
    output_table='public.my_game_report',
    output_mode='append',
    delete_condition="log_date = DATE '{{ logDate }}'",
    log_date='{{ ds }}',
    game_id='my_game',
)

start = EmptyOperator(task_id='start', dag=dag)
end   = EmptyOperator(task_id='end',   dag=dag)

start >> etl_task >> cons_task >> end
```

### DAG Conventions

| Setting           | Value                     | Why                                     |
|-------------------|---------------------------|-----------------------------------------|
| `catchup=True`    | Standard for daily DAGs   | Allows historical backfill              |
| `max_active_runs=1` | Always                  | Prevents concurrent writes to same path |
| `retries=2`       | Standard                  | Handles transient Spark/HDFS failures   |
| `start_date`      | First day of month        | Clean catchup boundary                  |
| `log_date`        | `'{{ ds }}'` or `'{{ macros.ds_add(ds, -1) }}'` | Use -1 for pipelines that process yesterday |

### logDate offset

Most game DAGs process the **previous day's data** because source systems write with a one-day lag:

```python
LOG_DATE = '{{ macros.ds_add(ds, -1) }}'
```

Pass this as `log_date=LOG_DATE` to every task in that DAG.

---

## 6. Connections and Credentials

### Connection Names

| Name            | System              | Use for                            |
|-----------------|---------------------|------------------------------------|
| `GDS_POSTGRES`  | GDS PostgreSQL      | Game operational data (ETL source) |
| `GDS_TRINO`     | Trino / Iceberg     | L2M Iceberg tables (ETL source)    |
| `TSN_POSTGRES`  | TSN PostgreSQL      | Business output tables (CONS dest) |

Connection credentials are stored as JSON files in HDFS:
```
hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
├── GDS_POSTGRES.json
├── GDS_TRINO.json
└── TSN_POSTGRES.json
```

`run_sql.sh` downloads the relevant credential file at runtime — you never handle credentials directly in DAG code.

### Google Service Account

The Google service account key (`tsn-data-*.json`) is also stored in
`hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/` and is used by `run_gsheet.sh` for Google Sheets access.

---

## 7. HDFS Path Conventions

HDFS base: `hdfs://c0s/user/gsbkk-workspace-yc9t6`

```
{base}/
├── archives/
│   ├── environment.tar.gz       # Python + PySpark env (uploaded once)
│   └── gsbkk-src.tar.gz         # Code tarball (rebuilt automatically at runtime)
├── configs/                     # Credentials (never committed to Git)
├── currency_mapping/            # Shared currency exchange rates
│
├── sensortower/
│   ├── raw/{endpoint}/{month}   # Raw API JSON
│   ├── etl/{endpoint}/{month}   # Cleaned Parquet
│   └── std/{endpoint}/{month}   # Standardised Parquet
│
├── game_health_check/{game}/
│   ├── active_details/{logDate}
│   ├── charge_details/{logDate}
│   ├── campaign/
│   └── std/{active,charge}/{logDate}
│
├── rfc/{game}/
│   ├── raw/daily/{logDate}
│   └── raw/monthly/{logDate}
│
└── ltv/{game}/
    ├── etl/base_users/{logDate}
    ├── std/{sessions,retention,purchase,...}/{logDate}
    └── cons/feature_set/{logDate}
```

**Path variables:** `{logDate}` in HDFS paths is replaced at runtime by `sql_runner.py`
when you pass `log_date=` to `create_sql_operator`.

---

## 8. Adding a New Pipeline

### Step 1 — Write the SQL templates

Create files in `transform/etl/`, `transform/std/`, `transform/cons/` (or `templates/` for
game-specific SQL shared across multiple DAGs).

Name convention: `{project}_{game}_{metric}.sql.j2`

```
transform/etl/my_game_active.sql.j2
transform/etl/my_game_charge.sql.j2
transform/std/my_game_charge_agg.sql.j2
transform/cons/my_game_report.sql.j2
```

### Step 2 — Test SQL directly

Connect to the source database in your SQL client (DBeaver, psql, Trino CLI) and run the
template with hard-coded date values. Only move to the next step once the query returns
expected data.

### Step 3 — Create the DAG

Create `dags/my_project/my_game_dag.py`. Follow the minimal template in Section 5.

### Step 4 — Test locally

```bash
cd /opt/airflow/dags/repo/gsbkk

# Test one task via the shell script directly
bash run_sql.sh \
    extract_sql_file=transform/etl/my_game_active.sql.j2 \
    extract_connection=GDS_POSTGRES \
    output_type=file \
    output_path=hdfs://c0s/user/gsbkk-workspace-yc9t6/my_game/etl/active/2026-03-01 \
    log_date=2026-03-01 \
    game_id=my_game

# Verify HDFS output
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/my_game/etl/active/2026-03-01
```

### Step 5 — Deploy

Push to Git. The company Airflow syncs from the repo automatically (see Section 9).

### Step 6 — Verify in Airflow UI

1. Open Airflow web UI.
2. Find `my_project_my_game` DAG (may take up to 5 minutes to appear after sync).
3. Enable the DAG toggle.
4. Trigger a manual run with a known-good date.
5. Check task logs if any step fails.

---

## 9. Deployment

### How it works

There is **no GitLab runner**. Deployment works like this:

1. You `git push` your changes.
2. Airflow pulls the repo to `/opt/airflow/dags/repo/gsbkk` (auto-sync, ~1-5 min).
3. When a DAG task runs, `run_sql.sh` rebuilds `gsbkk-src.tar.gz` from the current
   repo checkout and uploads it to HDFS before submitting the Spark job.

This means **the code tarball is always fresh** — you never need to manually push a
tarball or invalidate a cache.

### First-time HDFS setup

These one-time steps are needed only when setting up from scratch:

```bash
# 1. Create directory structure
hdfs dfs -mkdir -p hdfs://c0s/user/gsbkk-workspace-yc9t6/{archives,configs}

# 2. Upload Python environment (built once, rarely changes)
python3 -m venv gsbkk-env
source gsbkk-env/bin/activate
pip install -r requirements/base.txt
cd .. && tar -czf environment.tar.gz gsbkk-env/
hdfs dfs -put -f environment.tar.gz hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/

# 3. Upload JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
hdfs dfs -put -f postgresql-42.7.4.jar hdfs://c0s/user/gsbkk-workspace-yc9t6/

# 4. Upload credentials (never commit these to Git)
hdfs dfs -put -f GDS_POSTGRES.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f GDS_TRINO.json    hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f TSN_POSTGRES.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f tsn-data-*.json   hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
```

---

## 10. Debugging

### Read the Airflow task log first

In Airflow UI → DAG → Task Instance → "View Log". The log shows the full `spark-submit`
command and Spark stderr output.

### Re-run a shell script directly

Every Airflow task is ultimately a `run_sql.sh` call. You can reproduce any task by
copying its arguments and running the script on the Airflow server:

```bash
cd /opt/airflow/dags/repo/gsbkk

# Pattern C (JDBC extraction)
bash run_sql.sh \
    extract_sql_file=templates/game_health_check/l2m/active_details.sql.j2 \
    extract_connection=GDS_TRINO \
    output_type=file \
    output_path=hdfs://c0s/user/gsbkk-workspace-yc9t6/game_health_check/l2m/active_details/2026-03-16 \
    log_date=2026-03-16 \
    game_id=l2m

# Pattern E (multi-input CONS with secondary JDBC)
bash run_sql.sh \
    sql_file=templates/game_health_check/package_performance.sql.j2 \
    input_path=hdfs://.../charge_details/2026-03-16 \
    input_view=charge_details \
    secondary_sql_file=transform/etl/ghc_user_profile_l2m.sql.j2 \
    secondary_connection=GDS_POSTGRES \
    secondary_view=user_profile \
    output_type=jdbc \
    output_table=public.ghc_package_performance_l2m \
    output_mode=append \
    delete_condition="date = '2026-03-16'" \
    log_date=2026-03-16 \
    game_id=l2m
```

### Check HDFS output

```bash
# List files
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/game_health_check/l2m/active_details/2026-03-16

# Row count via Spark shell (quick sanity check)
spark-shell --master yarn
> spark.read.parquet("hdfs://c0s/.../active_details/2026-03-16").count()
```

### Check Postgres output

```bash
psql -h <tsn-host> -U <user> -d <db> \
    -c "SELECT COUNT(*), MIN(report_date), MAX(report_date) FROM public.ghc_diagnostic_daily_l2m"
```

### Common failures

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `FileNotFoundException` on HDFS input | Upstream task wrote to wrong path, or task ran before upstream completed | Check `{logDate}` substitution; verify task dependencies |
| `NullPointerException` in Spark | Mismatch between column names in SQL and actual Parquet schema | Run upstream task first, inspect schema with `spark.read.parquet(...).printSchema()` |
| Empty output table | Date filter too strict, or source data not yet available | Query source DB directly with the same SQL and date |
| Trino connection refused | `GDS_TRINO` credential file missing or wrong host | Check `hdfs dfs -cat hdfs://.../configs/GDS_TRINO.json` |
| `ClassNotFoundException: org.postgresql.Driver` | JDBC jar not in HDFS | Re-upload `postgresql-42.7.4.jar` (see Section 9) |

---

## 11. Active Projects

### Game Health Check

Daily KPI monitoring (DAU, revenue, retention) for gnoth, l2m, pwmsea and other games.

- **DAGs:** `dags/game_health_check/` — one file per game
- **SQL:** `templates/game_health_check/{game}/` for ETL/STD; shared `diagnostic_daily.sql.j2`, `package_performance.sql.j2`, `server_performance.sql.j2` for CONS
- **Outputs:** `public.ghc_diagnostic_daily_{game}`, `public.ghc_package_performance_{game}`, `public.ghc_server_performance_{game}` in TSN Postgres
- **Schedule:** 3–4 AM UTC daily

To add a new game: create SQL files in `templates/game_health_check/{game}/`, create a
secondary user_profile file in `transform/etl/ghc_user_profile_{game}.sql.j2`, and create
a new DAG file copying the gnoth pattern.

### SensorTower Market Research

Monthly extraction of competitor app rankings and metrics from the SensorTower API.

- **DAG:** `dags/sensortower/sensortower_dag.py`
- **SQL:** `transform/etl/` and `transform/std/` for SensorTower tables
- **API reference:** `docs/SENSORTOWER_QUICK_REFERENCE.md`
- **Schedule:** Monthly, 1st day at 2 AM UTC

### LTV Feature Set

Daily computation of LTV prediction features (sessions, retention, purchase, items, money,
targets) for the ML team.

- **DAG:** `dags/ltv/feature_set_dag.py`
- **SQL:** `transform/std/ltv_*.sql.j2` (6 feature files) + `transform/cons/ltv_feature_set.sql.j2`
- **Output:** `public.ltv_feature_set` in TSN Postgres

### Rolling Forecast (RFC)

Daily revenue actuals pipeline that feeds a Google Sheets dashboard.

- **DAGs:** `dags/rolling_forecast/` — per-game DAGs
- **SQL:** `transform/cons/rfc_daily.sql.j2`, `transform/cons/rfc_monthly.sql.j2`
- **Note:** The GSheet write-back step (efflux) is handled by `gsheet_processor.py` which
  is a separate process, not yet part of the `run_sql.sh` framework.

### Re-standardization (L2M, deferred)

Migrating L2M historical data to a unified schema. Currently deferred; JSON layouts in
`layouts/re-standardization/l2m/` are still in use by the legacy `run_etl_process.sh`
script until migration is complete.

---

*Last updated: 2026-03-17 — reflects post-simplification architecture (no JSON layout engine).*
