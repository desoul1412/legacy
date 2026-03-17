# Research & Technical Analysis: GSBKK Legacy vs. Simplified

## Analysis of Current Engine (`etl_engine.py`)

### Input Loading Mechanics
The current engine performs dynamic loading:
1. Grep/Replace substitution on JSON string.
2. Recursive variable resolution (`substitute_variables`).
3. Conditional logic for `type: file` vs `type: jdbc` vs `type: gsheet`.
4. Registering every input as a Spark Temp View.

**Simplification Opportunity**: Instead of a "Generic Engine", use a "Layered Schema". If data is consistently moved from `Raw` (JDBC/HDFS) to `ETL` (HDFS/Parquet) and then `STD` (Postgres), the transformation queries can be simplified to read from established tables rather than dynamic JSON-defined sources.

## Layering Strategy

| Layer | Responsibility | Storage | Format |
|-------|----------------|---------|--------|
| **RAW** | Mirror of source data | HDFS / S3 | JSON / Parquet / CSV |
| **ETL** | Flat, cleaned, typed data | DB / HDFS | Parquet / Table |
| **STD** | Standardized Dimensions/Metrics | Postgres (TSN) | Table |
| **CONS**| Consumer-ready aggregates | Postgres / Sheet | Table / CSV |

## Proposed "SQL-First" Pipeline Flow

1. **Extraction Task (Airflow)**: Use specific Operators (e.g., `SensorTowerExtractor`, `JdbcToHdfsOperator`) to land data in RAW.
2. **Transformation Task (Airflow)**: Execute SQL templates directly via `SparkSqlOperator` or a simplified `SqlRunner`.
   - The runner ONLY needs `input_table`, `sql_file`, and `output_table`.
   - No complex JSON layout.
   - Jinja2 still used for simple date substitution.

## Technical Trade-offs

### Approach A: dbt (Data Build Tool)
- **Pros**: Built-in lineage, documentation, and layering.
- **Cons**: Might be "too many new things" for the current infra; requires Spark/Postgres adapter setup.

### Approach B: Standard Airflow TaskFlow + SQL
- **Pros**: Uses existing Airflow; very simple SQL files.
- **Cons**: Still requires some boilerplate for loading RAW data into Spark if it's not yet in a catalog (like Iceberg/Hive).

### Approach C: Simplified "Lite" Engine
- **Pros**: Refactors `etl_engine.py` to remove JSON dependence.
- **Cons**: Less standardized than dbt.

## Findings from Code Review
- **LTV Feature Set** (`feature_set.sql.j2`): Is currently a monolithic 400-line query. It should be broken down into `etl_ltv_activity`, `etl_ltv_purchase`, then joined in the `cons` layer.
- **API Extractor**: Can be simplified by removing Spark dependencies inside the extraction logic where possible, focusing on landing the JSON raw first.
- **Python 2.7 Legacy**: The target environment is strictly Python 2.7. No modern features (f-strings, type hints, walrus operator, pathlib) can be used. All strings should be handled carefully for UTF-8 compatibility.
