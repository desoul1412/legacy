# Tech Stack: GSBKK Simplified Flow

## Data Infrastructure
- **Data Lake**: HDFS (existing) - Store RAW and ETL parquet files.
- **Compute**: Apache Spark 3.x (existing) - Core transformation engine.
- **Serving Layer**: PostgreSQL (TSN_POSTGRES) - Store STD and CONS layers for application use.
- **Orchestration**: Apache Airflow (existing) - Connecting tasks and scheduling.

## Software & Frameworks
- **SQL Templates**: Jinja2 - Used for dynamic SQL generation.
- **Language**: **Python 2.7** (Strict Core Requirement)
  - Follow the syntax used in `spark_processor.py`.
  - Use `key=value` argument parsing instead of complex `argparse`.
  - Use `.format()` for string interpolation.
- **Transformation Tool**: 
  - *Recommendation*: **Shared SQL Runner**. A simplified Python 2.7 script modeled after `spark_processor.py` that renders SQL via Jinja2 and executes it.

## Interface & Consumption
- **Business Interface**: Google Sheets (existing) - For configuration and business consumption.
- **Visualization**: Tableau / Metabase (connected to Postgres CONS layer).

## Credentials Handling
- **Path-Based Resolution**: Credentials are downloaded from HDFS to local temp directories during execution (as seen in `run_etl_process.sh`).
- **Standard Paths**:
  - `CREDENTIALS_DIR` (e.g., `/tmp/gsbkk_creds_$$/`)
  - `cred_tsn.json`, `cred_gds.json`, `gsheet_creds.json`.
- **HDFS Source**: `hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/`
