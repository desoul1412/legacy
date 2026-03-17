# Handoff: GSBKK Simplified Flow

## Current Status
The project is currently in the **Planning Phase**. Baseline analysis of the legacy `etl_engine.py` and JSON layout architecture has been completed. The proposed architecture and 10-week implementation plan are ready for review.

## Key Knowledge Points

### 1. The "Monolith" Problem
The most critical technical debt is the `feature_set.sql.j2` (400+ lines) which performs extraction, cleaning, and aggregation in one step. The new flow MUST enforce modularity:
- `etl_user_activity` -> `std_user_activity` -> `cons_ltv_features`.

### 2. Python 2.7 & Simplicity First
The new architecture must follow the design patterns used in `spark-runner.sh` and `spark_processor.py`:
- **Minimal Boilerplate**: Use simple positional shell arguments and `key=value` Python arguments.
- **SQL-Centric**: Logic resides in the SQL files, not in Python classes.
- **Direct Reading**: Use Spark's HDFS reading capabilities for configuration files.
- **No Pathlib/f-strings**: Maintain strict Python 2.7 compatibility.

### 3. Path-Based Credentials
Credentials are NOT managed as environment variables in Airflow. Instead:
- Shell scripts (`.sh`) download JSON credentials from HDFS to a temp folder via `hdfs dfs -get`.
- The `CREDENTIALS_DIR` environment variable is set dynamically at runtime to this temp folder.
- Python scripts must load JSON files from this path.

### 3. CI/CD and Permissions
- **Deployment**: Automatic sync from GitLab to Airflow workers every 10 minutes (approx).
- **Control**: All pipeline logic MUST be in the repository. No manual changes in the Airflow UI (except triggering DAGs).
- **Permissions**: Developer has Web View Only. Debugging relies exclusively on `stdout` from the Spark/Python scripts visible in the Airflow task logs.

## Next Steps for Development
1. **Prototype the SQL Runner**: Create a 50-line script that can run a `.sql` file on Spark.
2. **First Migration**: Pick a small pipeline (e.g., `gsheet_rfc_import`) and migrate it to the layered approach.
3. **Validate Postgres Writes**: Ensure the "Delete then Insert" idempotent logic from `etl_engine.py` is successfully ported to the new runner (or handled by Airflow SQL Operators).

## Contact & Ownership
- **Lead Architect**: [Antigravity AI]
- **Repository**: [gsbkk]
