# Implementation Plan: GSBKK Simplification

## Phase 1: Foundation & "Raw" Layer (Week 1-2)
- [ ] **Infrastructure Setup**: Create schemas/folders for `raw`, `etl`, `std`, `cons` layers in HDFS and Postgres.
- [ ] **GitLab Sync Setup**: Configure GitLab CI for automatic sync to Airflow directory.
- [ ] **Sync Verification**: Verify that a change in `dags/` is reflected in Airflow Web UI within 5 minutes.
- [x] **Templated API Action**: `src/api_to_raw.py` + `run_api.sh` — Python 2.7, `key=value` args, Jinja2 URL templates, HDFS write via subprocess. `create_raw_api_operator()` in `dag_helpers.py`.
- [x] **Lightweight SQL Runner**: `src/sql_runner.py` — Python 2.7, `key=value` args, Jinja2 SQL templates, 5 input patterns (single file, multi-file, JDBC extract, file+JDBC secondary, multi-file+JDBC secondary).
- [x] **Shell Wrapper**: `run_sql.sh`, `run_api.sh`, `run_gsheet.sh` — always rebuild code tarball at runtime (no stale cache).

## Phase 2: ETL & Normalization (Week 3-4)
- [x] **Migrate SensorTower ETL**: 5 ETL SQL files + 4 STD SQL files in `transform/`. `sensortower_dag.py` rewritten to use `create_sql_operator()`.
- [x] **Standardize Types**: Type casting (Dates, Decimals, Strings) done in each ETL SQL template.
- [ ] **Validation**: Compare outputs of new SQL flow vs. old JSON flow (operational).

## Phase 3: Re-Standardization (STD) Layer (Week 5-6)
- [ ] **Modularize Standard Activity**: Skipped — L2M re-standardization deferred.
- [ ] **Economy Standardization**: Skipped — L2M re-standardization deferred.
- [ ] **Remove Layouts**: `layouts/re-standardization/` still present (not migrated).

## Phase 4: Consolidation (CONS) & Business Logic (Week 7-8)
- [x] **Refactor LTV Monolith**: `templates/ltv/feature_set.sql.j2` split into 6 STD files + 1 CONS file in `transform/`. `feature_set_dag.py` rewritten: legacy STD tasks kept, new `LTV_STD` task group added using `create_sql_operator`, CONS replaced with `transform/cons/ltv_feature_set.sql.j2`.
- [x] **Rolling Forecast Migration**: `gsheet_rfc_import_dag.py` + `ncv_gsheet_rfc_import_dag.py` rewritten. `gsheet_to_raw.py` + `run_gsheet.sh` added. `transform/cons/rfc_daily.sql.j2` + `rfc_monthly.sql.j2` created. `create_gsheet_raw_operator()` in `dag_helpers.py`.
- [x] **Google Sheets Influx**: GSheet → HDFS RAW handled by `gsheet_to_raw.py`. Efflux (write-back to sheets) remains in `gsheet_processor.py` (out of scope for now).

## Phase 5: Handoff & Cleanup (Week 9-10)
- [x] **Airflow DAG Updates**: Game health check DAGs (gnoth, l2m, pwmsea) rewritten to use `create_sql_operator`. ETL/STD tasks use Pattern C (extract_sql_file + extract_connection); CONS tasks use Pattern E (input_paths + secondary_sql_file for user_profile). Per-game user_profile SQL files added to `transform/etl/`. RFC game DAGs (gnoth, l2m, pwmsea `*_daily_actual_rfc`) deferred — output is GSheet efflux, which requires `gsheet_processor.py` migration (out of scope).
- [x] **Documentation**: `transform/README.md` — "How to add a new pipeline" guide covering all 5 input patterns with examples.
- [x] **Engine Deprecation (partial)**: Deleted 46 layout JSON files (73 → 27, 63% reduction). Remaining `layouts/` files are all legitimately in use by deferred DAGs: `re-standardization/l2m/` (L2M deferred), `rolling_forecast/*/metrics_gsheet.json` (gsheet efflux deferred), `sensortower/cons/` + `mf/` (CONS not yet migrated), `common/currency_mapping.json` (active). `etl_engine.py` and `run_etl_process.sh` still needed by those DAGs.

## Success Criteria
- [x] 50% Reduction in JSON configuration files. (73 → 27 files, 63% reduction)
- [x] Improved debuggability (SQL queries are runnable directly in Trino/SparkSQL).
- [ ] Faster onboarding for new analysts.
- [x] Lineage visibility (Data moves clearly through Raw -> ETL -> STD -> CONS).
