# Functions

This page documents all custom functions in the `functions/` folder.

## airflow.py

- **airflow_callback(context: Dict)**: Handles Airflow failure callbacks. Extracts DAG info, checks if error already logged, sends to MSTeams and Postgres if new.
- **get_dag_info(context: Dict) -> Dict**: Extracts DAG execution information from Airflow context.
- **check_airflow_error_log(error_info)**: Checks if the error is already logged in the database.
- **send_error_data(error_info: Dict)**: Sends error notification to MSTeams and logs to Postgres.
- **create_airflow_attachment(error_info: Dict) -> Dict**: Creates MSTeams message attachment for error.
- **send_error_info_to_postgres(error_info: Dict)**: Inserts error details into Postgres database.

## at.py

- **add_operators(dag, game_id, log_date, pipelines, dag_done_flag)**: Adds various bash operators for the AT (Attribution?) pipeline to the DAG, including load, standardize, consolidate tasks.

## at_bi.py

- **add_operators(dag, game_id, log_date, pipelines, dag_done_flag)**: Similar to at.py, adds operators for BI pipeline.

## common.py

- **start_from(year, month, day, hour, minute = 0)**: Calculates start date, cron interval, and log date for DAG scheduling.
- **build_dag(game_id, dag_type, start_date, interval)**: Creates and returns a DAG object with given parameters.
- **get_default_operator_args(dag, retries = 0, retry_delay = 20)**: Returns default arguments for Airflow operators.
- **dummy_operator(dag, task_id, game_id, log_date)**: Creates a dummy (no-op) operator.
- **bash_operator(dag, task_id, game_id, log_date, action, layouts, retries = 0, retry_delay = 20)**: Creates a bash operator for MDM runner scripts.
- **create_bash_operator(dag, task_id, game_id, log_date, pipelines, retries = 0, retry_delay = 20)**: Creates bash operator from pipelines dictionary.
- **check_mark_done_operator(dag, task_id, game_id, log_date, flag_name, retries = 5, retry_delay = 20)**: Creates operator to check if a task is marked as done.
- **check_bi_done_operator(dag, task_id, game_id, log_date, flag_name, retries = 5, retry_delay = 20)**: Checks BI completion status.
- **mark_done_operator(dag, task_id, game_id, log_date, flag_name)**: Marks a task as completed.
- **interval_operator(dag, task_id, game_id, log_date, action, layouts, log_time, retries = 0, retry_delay = 0, depends_on_past=False, wait_for_downstream=False)**: Creates operator for interval-based tasks.
- **create_interval_operator(dag, task_id, game_id, log_date, pipelines, log_time, retries = 0, retry_delay = 0)**: Creates interval operator from pipelines.
- **bash_operator_testmode(dag, task_id, game_id, log_date, action, layouts, retries = 0, retry_delay = 20)**: Bash operator in test mode.
- **create_bash_operator_testmode(dag, task_id, game_id, log_date, pipelines, retries = 0, retry_delay = 20)**: Test mode bash operator from pipelines.
- **bash_operator_large(dag, task_id, game_id, log_date, action, layouts, retries = 0, retry_delay = 20)**: Bash operator for large jobs.
- **create_bash_operator_large(dag, task_id, game_id, log_date, pipelines, retries = 0, retry_delay = 20)**: Large job bash operator from pipelines.

## constants.py

No functions, contains classes and constants for credentials, table paths, etc.

## data_connection.py

Class **DataConnection**:
- **execute_query(qlstring)**: Executes a SQL query and returns results.
- **execute_batch(qlstring, data)**: Executes batch SQL inserts/updates.

## data_extraction.py

Class **DataExtraction** (factory):
- **extract_data()**: Factory method to create appropriate extractor.

Class **PostgresDataExtractor**:
- **set_where(where)**: Sets warehouse (gds/tsn).
- **set_game_id(game_id)**: Sets game ID.
- **set_content(content)**: Sets content type (active, charge, etc.).
- **set_country_code(country_code)**: Sets country filter.
- **set_start_date(start_date)**: Sets start date.
- **set_end_date(end_date)**: Sets end date.
- **set_log_date(log_date)**: Sets log date.
- **set_get_details(get_details)**: Sets details flag.
- **extract()**: Executes the data extraction.

Similar classes for HadoopDataExtractor, GGSheetDataExtractor.

## data_processors.py

- **get_execution_metadata()**: Returns common execution metadata (timestamp, environment, version).
- **process_data(data: dict) -> dict**: Sample data processing function, adds metadata to data.

## database.py

Class **DataConnection**:
- **create_connection()**: Creates database connection based on type.
- **create_postgres_connection(cred_type)**: Creates Postgres JDBC connection for Spark.

## file.py

- **get_file_names(folder_path)**: Returns list of file names (without extensions) in a folder.

## game_configs.py

Class **GameConfig**:
- **_load_config()**: Loads column standardization config from YAML.
- **_get_standard_columns(content)**: Gets standard columns for content type.
- **standardize(df: SparkDataFrame, content: str) -> SparkDataFrame**: Renames columns to standard names.

## notification.py

- **send_message(webhook: str, attachments: Dict)**: Sends message to MSTeams webhook.

## path.py

No functions, contains Path class with file path constants.

## pipeline.py

- **start_from(year, month, day, hour, minute, mode)**: Calculates start date, interval, log date, and date range for different modes (daily/weekly/monthly).
- **build_dag(game_id, dag_type, start_date, interval)**: Creates DAG object.

## pipeline_test.py

Similar functions for test pipelines.

## spark_singleton.py

Class **SparkSingleton**:
- **get_spark(app_name="DefaultApp")**: Returns singleton Spark session.
- **stop_spark()**: Stops the Spark session.

## sql.py

Class **SQLHandler**:
- **build_query(where, content, game_id, country_code, start_date, end_date)**: Builds SQL query from template or custom file.
- **_get_query_file(content)**: Gets SQL file path.
- **_prepare_template_vars(...)**: Prepares Jinja2 template variables.
- **_render_template(sql_file_path, template_vars)**: Renders SQL template.
- **get_custom_query(content)**: Gets custom SQL query.
- **get_template_query(content)**: Gets template SQL query.

## trigger_sql.py

- **create_trigger_sql_operator(dag, game_id, event_name, sql_file, log_date, log_time)**: Creates Python operator for SQL triggers.
- **trigger(game_id, sql_file, log_date, log_time)**: Executes SQL trigger on database.

## warehouse.py

- **get_currency_mapping()**: Retrieves currency exchange rates from database.
- **write_to_db(df, dbname, dbtable, mode)**: Writes Spark DataFrame to Postgres database.
