# Transform Layer — How to Add a New Pipeline

SQL transformation files for the GSBKK simplified pipeline.
No JSON layout files. No ETL engine. Just SQL + a shell call in the DAG.

## Directory Structure

```
transform/
├── etl/    Clean, type-cast, filter raw source data → HDFS Parquet
├── std/    Standardise into common schemas           → HDFS Parquet
└── cons/   Aggregate for business use cases          → Postgres TSN
```

## Layer Overview

| Layer | Input | Output | Shell script |
|-------|-------|--------|--------------|
| RAW   | External API / Google Sheet | HDFS JSON | `run_api.sh` / `run_gsheet.sh` |
| ETL   | HDFS raw / Trino JDBC | HDFS Parquet | `run_sql.sh` |
| STD   | HDFS Parquet (multiple) | HDFS Parquet | `run_sql.sh` |
| CONS  | HDFS Parquet + optional JDBC | Postgres TSN | `run_sql.sh` |

---

## Pattern 1 — API source (e.g. SensorTower, any HTTP endpoint)

```python
# DAG task
from utils.dag_helpers import create_raw_api_operator, create_sql_operator

raw = create_raw_api_operator(
    dag=dag, task_id='raw_my_endpoint',
    url_template='https://api.example.com/v1/data?date={{ logDate }}',
    cred_file='cred_tsn.json', cred_key='my_api',
    response_key='data',   # dot-path to the list in the JSON response
    paginate=True,
    output_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/my_source/endpoint/{logDate}',
)

etl = create_sql_operator(
    dag=dag, task_id='etl_my_endpoint',
    sql_file='transform/etl/my_source_endpoint.sql.j2',
    output_type='file',
    input_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/my_source/endpoint/{logDate}',
    output_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/my_source/etl/endpoint/{logDate}',
)
raw >> etl
```

---

## Pattern 2 — Google Sheet source (e.g. Rolling Forecast)

```python
from utils.dag_helpers import create_gsheet_raw_operator, create_sql_operator

raw = create_gsheet_raw_operator(
    dag=dag, task_id='raw_daily_rfc',
    sheet_id='<spreadsheet_id>',
    worksheet='Daily Overall',
    output_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/rfc/daily/{logDate}',
)

cons = create_sql_operator(
    dag=dag, task_id='cons_daily_rfc',
    sql_file='transform/cons/rfc_daily.sql.j2',
    output_type='jdbc',
    input_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/rfc/daily/{logDate}',
    input_view='daily_rfc',
    output_table='public.rfc_daily',
    output_mode='append',
    delete_condition="(date IS NULL OR game IS NULL) OR EXTRACT(YEAR FROM date) = {{ ds[:4] }}",
)
raw >> cons
```

---

## Pattern 3 — Trino extraction (game data already in Iceberg)

```python
etl = create_sql_operator(
    dag=dag, task_id='etl_my_game_metric',
    extract_sql_file='transform/etl/my_game_metric.sql.j2',
    extract_connection='GDS_TRINO',
    output_type='file',
    output_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/my_game/etl/metric/{logDate}',
)
```

SQL (`transform/etl/my_game_metric.sql.j2`):
```sql
SELECT actor_str3 AS user_id, actor_world AS server_id, use_num1 AS amount
FROM iceberg.my_game.etl_increase_money
WHERE entity_id = '900000001'
  AND ds = DATE '{{ logDate }}'
```

---

## Pattern 4 — CONS with secondary JDBC lookup

```python
cons = create_sql_operator(
    dag=dag, task_id='cons_my_enriched',
    sql_file='transform/cons/my_enriched.sql.j2',
    output_type='jdbc',
    input_path='hdfs://c0s/...',      input_view='main_data',
    secondary_sql_file='transform/cons/my_lookup.sql.j2',
    secondary_connection='GDS_POSTGRES', secondary_view='lookup',
    output_table='public.my_enriched',
    output_mode='append',
    delete_condition="report_date = DATE '{{ logDate }}'",
)
```

---

## Pattern 5 — STD UNION (multiple HDFS inputs)

```python
std = create_sql_operator(
    dag=dag, task_id='std_active',
    sql_file='transform/std/active.sql.j2',
    output_type='file',
    input_paths='login|hdfs://.../etl/login/{logDate};logout|hdfs://.../etl/logout/{logDate}',
    output_path='hdfs://.../std/active/{logDate}',
)
```

---

## Jinja2 Variables Available in SQL Templates

| Variable   | Example        | Notes                   |
|------------|----------------|-------------------------|
| `logDate`  | `2026-03-17`   | Primary date variable   |
| `log_date` | `2026-03-17`   | Alias, always set       |
| `gameId`   | `l2m`          | Set via `game_id=` arg  |
| `game_id`  | `l2m`          | Alias, always set       |

Import macros in a template:
```sql
{% import 'macros.sql' as macros %}
... {{ macros.date_filter("DATE(logtime + INTERVAL '7' HOUR)", logDate) }} ...
```

---

## Shell Scripts Reference

| Script | Purpose | Uses Spark? |
|--------|---------|-------------|
| `run_sql.sh` | SQL transform (ETL / STD / CONS) | Yes |
| `run_api.sh` | HTTP API → HDFS RAW | No |
| `run_gsheet.sh` | Google Sheet → HDFS RAW | No |
| `run_pipeline.sh` | Complex multi-step pipelines (legacy) | Yes |
| `run_etl_process.sh` | JSON-layout ETL (legacy) | Yes |
