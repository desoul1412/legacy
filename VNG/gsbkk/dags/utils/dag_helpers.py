"""
DAG Helper Utilities

Simplified functions for creating Airflow tasks with declarative pipeline configurations.
Reduces boilerplate code and makes DAG definitions more readable.

Author: GSBKK Team
Date: 2025-12-26

Example Usage:
    >>> from utils.dag_helpers import create_etl_operator, create_pipeline_tasks
    >>> 
    >>> # Define pipeline as simple dict
    >>> pipeline = [
    >>>     {'name': 'extract_active', 'type': 'etl', 'layout': 'layouts/rfc/etl/active.json'},
    >>>     {'name': 'extract_charge', 'type': 'etl', 'layout': 'layouts/rfc/etl/charge.json'},
    >>>     {'name': 'consolidate', 'type': 'cons', 'layout': 'layouts/rfc/cons/metrics.json'},
    >>> ]
    >>> 
    >>> # Create all tasks at once
    >>> tasks = create_pipeline_tasks(dag, pipeline, game_id='cft')
    >>> 
    >>> # Or create individual operators
    >>> task = create_etl_operator(dag, 'extract_data', 'layouts/etl/data.json', game_id='cft')
"""

from airflow.operators.bash import BashOperator
from typing import Dict, List, Optional


def create_etl_operator(
    dag,
    task_id: str,
    layout: str,
    game_id: str,
    log_date: str = "{{ ds }}",
    vars: str = "",
    script_base: str = '/opt/airflow/dags/repo',
    **kwargs
) -> BashOperator:
    """
    Create a BashOperator for ETL tasks using run_etl_process.sh
    
    Args:
        dag: Airflow DAG object
        task_id: Task identifier
        layout: Path to layout JSON file (relative to repo root)
        game_id: Game identifier
        log_date: Airflow date template (default: {{ ds }})
        vars: Additional variables as comma-separated key=value pairs
        script_base: Base path to scripts (default: /opt/airflow/dags/repo)
        **kwargs: Additional BashOperator parameters
    
    Returns:
        BashOperator instance
    """
    vars_flag = f"--vars {vars}" if vars else ""
    
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            cd {script_base}
            ./run_etl_process.sh \
                --layout {layout} \
                --gameId {game_id} \
                --logDate {log_date} \
                {vars_flag}
        """,
        dag=dag,
        **kwargs
    )


def create_raw_api_operator(
    dag,
    task_id,
    url_template,
    output_path,
    log_date="{{ ds }}",
    params="",
    cred_file="",
    cred_key="",
    auth_param="auth_token",
    auth_header="",
    auth_value_key="api_key",
    response_key="",
    paginate=False,
    page_param="offset",
    page_size=10000,
    method="GET",
    timeout=60,
    max_retries=3,
    script_base='/opt/airflow/dags/repo',
    **kwargs
):
    """
    Create a BashOperator that calls run_api.sh / api_to_raw.py.

    Fetches data from an external HTTP API and writes raw JSON lines to HDFS.
    No Spark required — runs with the local Python environment from the archive.

    Args:
        dag: Airflow DAG object
        task_id: Task identifier
        url_template: API URL; may contain {{ logDate }} Jinja2 placeholders
        output_path: HDFS destination; may contain {logDate} substitution tokens
        log_date: Airflow date template (default: {{ ds }})
        params: JSON-encoded dict of extra query params, e.g. '{"limit":"500"}'
        cred_file: Credential filename in HDFS configs (e.g. cred_tsn.json)
        cred_key: Key within the credential file (e.g. sensortower)
        auth_param: Query param name for the token (default: auth_token)
        auth_header: Header name for the token (alternative to auth_param)
        auth_value_key: Key in creds dict holding the token (default: api_key)
        response_key: Dot-path into response JSON to reach the data list
        paginate: Enable offset-based pagination (default: False)
        page_param: Offset param name (default: offset)
        page_size: Records per page (default: 10000)
        method: HTTP method GET or POST (default: GET)
        timeout: Request timeout in seconds (default: 60)
        max_retries: Max retries on rate-limit/server errors (default: 3)
        script_base: Base path to scripts

    Returns:
        BashOperator instance

    Example:
        >>> extract_new_games = create_raw_api_operator(
        ...     dag=dag,
        ...     task_id='raw_new_games_ios',
        ...     url_template='https://api.sensortower.com/v1/ios/apps/app_ids?category=6014&start_date={{ logDate }}&limit=10000',
        ...     cred_file='cred_tsn.json',
        ...     cred_key='sensortower',
        ...     response_key='ids',
        ...     paginate=True,
        ...     output_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/sensortower/new_games_ios/{logDate}',
        ... )
    """
    args = [
        'url_template={0}'.format(url_template),
        'output_path={0}'.format(output_path),
        'logDate={0}'.format(log_date),
    ]

    if params:
        args.append('params="{0}"'.format(params))
    if cred_file:
        args.append('cred_file={0}'.format(cred_file))
    if cred_key:
        args.append('cred_key={0}'.format(cred_key))
    if auth_param and auth_param != 'auth_token':
        args.append('auth_param={0}'.format(auth_param))
    if auth_header:
        args.append('auth_header={0}'.format(auth_header))
    if auth_value_key and auth_value_key != 'api_key':
        args.append('auth_value_key={0}'.format(auth_value_key))
    if response_key:
        args.append('response_key={0}'.format(response_key))
    if paginate:
        args.append('paginate=true')
    if page_param and page_param != 'offset':
        args.append('page_param={0}'.format(page_param))
    if page_size != 10000:
        args.append('page_size={0}'.format(page_size))
    if method and method != 'GET':
        args.append('method={0}'.format(method))
    if timeout != 60:
        args.append('timeout={0}'.format(timeout))
    if max_retries != 3:
        args.append('max_retries={0}'.format(max_retries))

    args_str = ' \\\n                '.join(args)

    return BashOperator(
        task_id=task_id,
        bash_command="""
            cd {base}
            ./run_api.sh \\
                {args}
        """.format(base=script_base, args=args_str),
        dag=dag,
        **kwargs
    )


def create_gsheet_raw_operator(
    dag,
    task_id,
    sheet_id,
    worksheet,
    output_path,
    log_date="{{ ds }}",
    cred_file="tsn-data-0e06f020fc9b.json",
    skip_empty=True,
    script_base='/opt/airflow/dags/repo',
    **kwargs
):
    """
    Create a BashOperator that calls run_gsheet.sh / gsheet_to_raw.py.

    Reads a Google Sheets worksheet and writes raw JSON lines to HDFS.
    No Spark required — runs with the local Python environment from the archive.

    Args:
        dag: Airflow DAG object
        task_id: Task identifier
        sheet_id: Google Sheets document ID (from the URL)
        worksheet: Worksheet (tab) name, e.g. "Daily Overall"
        output_path: HDFS destination; may contain {logDate} substitution tokens
        log_date: Airflow date template (default: {{ ds }})
        cred_file: Service-account JSON filename in HDFS configs
        skip_empty: Skip rows where all cells are empty (default: True)
        script_base: Base path to scripts

    Returns:
        BashOperator instance

    Example:
        >>> raw_daily = create_gsheet_raw_operator(
        ...     dag=dag,
        ...     task_id='raw_daily_rfc',
        ...     sheet_id='1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w',
        ...     worksheet='Daily Overall',
        ...     output_path='hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/rfc/daily/{logDate}',
        ... )
    """
    args = [
        'sheet_id={0}'.format(sheet_id),
        'worksheet={0}'.format(worksheet),
        'output_path={0}'.format(output_path),
        'logDate={0}'.format(log_date),
    ]

    if cred_file and cred_file != 'tsn-data-0e06f020fc9b.json':
        args.append('cred_file={0}'.format(cred_file))
    if not skip_empty:
        args.append('skip_empty=false')

    args_str = ' \\\n                '.join(args)

    return BashOperator(
        task_id=task_id,
        bash_command="""
            cd {base}
            ./run_gsheet.sh \\
                {args}
        """.format(base=script_base, args=args_str),
        dag=dag,
        **kwargs
    )


# create_gsheet_operator() removed - referenced deleted run_gsheet_process.sh
# Google Sheets functionality is handled by etl_engine.py with type='gsheet' in layouts


def create_pipeline_operator(
    dag,
    task_id: str,
    script_name: str,
    params: List[str],
    stage: Optional[str] = None,
    script_base: str = '/opt/airflow/dags/repo',
    **kwargs
) -> BashOperator:
    """
    Create a BashOperator for Python pipeline tasks using run_pipeline.sh
    
    Args:
        dag: Airflow DAG object
        task_id: Task identifier
        script_name: Python script name (e.g., 'monthly_data.ai.py')
        params: List of parameters to pass to script
        stage: Optional stage name (extract, process, transform, etc.)
        script_base: Base path to scripts
        **kwargs: Additional BashOperator parameters
    
    Returns:
        BashOperator instance
    """
    all_params = params + ([stage] if stage else [])
    params_str = ' '.join(all_params)
    
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            cd {script_base}
            ./run_pipeline.sh {script_name} {params_str}
        """,
        dag=dag,
        **kwargs
    )


def create_sql_operator(
    dag,
    task_id,
    output_type,
    log_date="{{ ds }}",
    sql_file="",
    extract_sql_file="",
    extract_connection="",
    extract_view="source_data",
    input_path="",
    input_format="parquet",
    input_view="source_data",
    input_paths="",
    secondary_sql_file="",
    secondary_connection="",
    secondary_view="secondary",
    output_table="",
    output_path="",
    output_connection="TSN_POSTGRES",
    output_mode="overwrite",
    delete_condition="",
    num_partitions=0,
    game_id="",
    extra_vars="",
    script_base='/opt/airflow/dags/repo',
    **kwargs
):
    """
    Create a BashOperator for SQL-first transformation tasks using run_sql.sh.

    This is the new-style operator that replaces create_etl_operator for
    SQL-centric pipelines (no JSON layout file required).

    Args:
        dag: Airflow DAG object
        task_id: Task identifier
        sql_file: Path to .sql.j2 file (relative to repo root)
        output_type: 'jdbc' or 'file'
        log_date: Airflow date template (default: {{ ds }})
        output_table: Target Postgres table (e.g. 'public.rfc_daily')
        output_path: Target HDFS path (for output_type='file')
        output_connection: 'TSN_POSTGRES' or 'GDS_POSTGRES'
        output_mode: 'overwrite' or 'append'
        delete_condition: SQL WHERE clause for idempotent delete+insert
        input_path: Optional HDFS path to register as a Spark temp view
        input_view: Temp view name for input_path (default: 'source')
        game_id: Game identifier (used for JDBC database selection)
        extra_vars: Additional key=value pairs as a space-separated string
        script_base: Base path to scripts
        **kwargs: Additional BashOperator parameters

    Returns:
        BashOperator instance

    Example:
        >>> cons_rfc = create_sql_operator(
        ...     dag=dag,
        ...     task_id='cons_rfc_daily',
        ...     sql_file='transform/rolling_forecast/cons/rfc_daily.sql.j2',
        ...     output_type='jdbc',
        ...     output_table='public.rfc_daily',
        ...     output_mode='append',
        ...     delete_condition='EXTRACT(YEAR FROM date) = 2026',
        ...     game_id='rfc',
        ... )
    """
    args = [
        'output_type={0}'.format(output_type),
        'logDate={0}'.format(log_date),
    ]

    if sql_file:
        args.append('sql_file={0}'.format(sql_file))
    if extract_sql_file:
        args.append('extract_sql_file={0}'.format(extract_sql_file))
    if extract_connection:
        args.append('extract_connection={0}'.format(extract_connection))
    if extract_view and extract_view != 'source_data':
        args.append('extract_view={0}'.format(extract_view))
    if input_path:
        args.append('input_path={0}'.format(input_path))
    if input_format and input_format != 'parquet':
        args.append('input_format={0}'.format(input_format))
    if input_view and input_view != 'source_data':
        args.append('input_view={0}'.format(input_view))
    if input_paths:
        args.append('input_paths="{0}"'.format(input_paths))
    if secondary_sql_file:
        args.append('secondary_sql_file={0}'.format(secondary_sql_file))
    if secondary_connection:
        args.append('secondary_connection={0}'.format(secondary_connection))
    if secondary_view and secondary_view != 'secondary':
        args.append('secondary_view={0}'.format(secondary_view))
    if output_table:
        args.append('output_table={0}'.format(output_table))
    if output_path:
        args.append('output_path={0}'.format(output_path))
    if output_connection and output_connection != 'TSN_POSTGRES':
        args.append('output_connection={0}'.format(output_connection))
    if output_mode and output_mode != 'overwrite':
        args.append('output_mode={0}'.format(output_mode))
    if delete_condition:
        args.append('delete_condition="{0}"'.format(delete_condition))
    if num_partitions > 0:
        args.append('num_partitions={0}'.format(num_partitions))
    if game_id:
        args.append('gameId={0}'.format(game_id))
    if extra_vars:
        args.append(extra_vars)

    args_str = ' \\\n                '.join(args)

    return BashOperator(
        task_id=task_id,
        bash_command="""
            cd {base}
            ./run_sql.sh \\
                {args}
        """.format(base=script_base, args=args_str),
        dag=dag,
        **kwargs
    )


def create_pipeline_tasks(
    dag,
    pipeline_config: List[Dict],
    game_id: Optional[str] = None,
    log_date: str = "{{ ds }}",
    script_base: str = '/opt/airflow/dags/repo',
    **kwargs
) -> Dict[str, BashOperator]:
    """
    Create multiple operators from a pipeline configuration list.
    
    Args:
        dag: Airflow DAG object
        pipeline_config: List of task configurations
            Each config should have:
            - name: Task ID
            - type: Task type ('etl', 'api', 'gsheet', 'pipeline')
            - layout: Layout file path (for etl/api/gsheet)
            - script: Script name (for pipeline type)
            - params: Script parameters (for pipeline type)
            - stage: Optional stage name (for pipeline type)
        game_id: Default game identifier
        log_date: Default log date template
        script_base: Base path to scripts
        **kwargs: Additional BashOperator parameters (applied to all tasks)
    
    Returns:
        Dictionary mapping task names to BashOperator instances
    
    Example:
        >>> # Game health check pipeline (ETL → STD → CONS)
        >>> pipeline = [
        >>>     {'name': 'etl_active', 'type': 'etl', 'layout': 'layouts/game_health_check/gnoth/etl/active.json'},
        >>>     {'name': 'std_active', 'type': 'etl', 'layout': 'layouts/game_health_check/gnoth/std/active.json'},
        >>>     {'name': 'cons_diagnostic', 'type': 'etl', 'layout': 'layouts/game_health_check/gnoth/cons/diagnostic_daily.json'},
        >>> ]
        >>> # Rolling forecast pipeline (single CONS step)
        >>> rfc_pipeline = [
        >>>     {'name': 'cons_metrics', 'type': 'etl', 'layout': 'layouts/rolling_forecast/gnoth/cons/metrics.json'},
        >>> ]
        >>> tasks = create_pipeline_tasks(dag, pipeline, game_id='gnoth')
        >>> tasks['etl_active'] >> tasks['std_active'] >> tasks['cons_diagnostic']
    """
    tasks = {}
    
    for config in pipeline_config:
        task_name = config['name']
        task_type = config['type']
        
        # Merge config-specific kwargs with global kwargs
        task_kwargs = {**kwargs, **config.get('kwargs', {})}
        
        if task_type == 'etl':
            task = create_etl_operator(
                dag=dag,
                task_id=task_name,
                layout=config['layout'],
                game_id=config.get('game_id', game_id),
                log_date=config.get('log_date', log_date),
                vars=config.get('vars', ''),
                script_base=script_base,
                **task_kwargs
            )
        elif task_type == 'api':
            task = create_api_operator(
                dag=dag,
                task_id=task_name,
                layout=config['layout'],
                month=config.get('month', "{{ execution_date.strftime('%Y-%m') }}"),
                script_base=script_base,
                **task_kwargs
            )
        elif task_type == 'gsheet_raw':
            task = create_gsheet_raw_operator(
                dag=dag,
                task_id=task_name,
                sheet_id=config['sheet_id'],
                worksheet=config['worksheet'],
                output_path=config['output_path'],
                log_date=config.get('log_date', log_date),
                cred_file=config.get('cred_file', 'tsn-data-0e06f020fc9b.json'),
                skip_empty=config.get('skip_empty', True),
                script_base=script_base,
                **task_kwargs
            )
        elif task_type == 'raw_api':
            task = create_raw_api_operator(
                dag=dag,
                task_id=task_name,
                url_template=config['url_template'],
                output_path=config['output_path'],
                log_date=config.get('log_date', log_date),
                params=config.get('params', ''),
                cred_file=config.get('cred_file', ''),
                cred_key=config.get('cred_key', ''),
                auth_param=config.get('auth_param', 'auth_token'),
                auth_header=config.get('auth_header', ''),
                auth_value_key=config.get('auth_value_key', 'api_key'),
                response_key=config.get('response_key', ''),
                paginate=config.get('paginate', False),
                page_param=config.get('page_param', 'offset'),
                page_size=config.get('page_size', 10000),
                method=config.get('method', 'GET'),
                timeout=config.get('timeout', 60),
                max_retries=config.get('max_retries', 3),
                script_base=script_base,
                **task_kwargs
            )
        elif task_type == 'sql':
            task = create_sql_operator(
                dag=dag,
                task_id=task_name,
                output_type=config.get('output_type', 'file'),
                log_date=config.get('log_date', log_date),
                sql_file=config.get('sql_file', ''),
                extract_sql_file=config.get('extract_sql_file', ''),
                extract_connection=config.get('extract_connection', ''),
                extract_view=config.get('extract_view', 'source_data'),
                input_path=config.get('input_path', ''),
                input_format=config.get('input_format', 'parquet'),
                input_view=config.get('input_view', 'source_data'),
                input_paths=config.get('input_paths', ''),
                secondary_sql_file=config.get('secondary_sql_file', ''),
                secondary_connection=config.get('secondary_connection', ''),
                secondary_view=config.get('secondary_view', 'secondary'),
                output_table=config.get('output_table', ''),
                output_path=config.get('output_path', ''),
                output_connection=config.get('output_connection', 'TSN_POSTGRES'),
                output_mode=config.get('output_mode', 'overwrite'),
                delete_condition=config.get('delete_condition', ''),
                num_partitions=config.get('num_partitions', 0),
                game_id=config.get('game_id', game_id or ''),
                extra_vars=config.get('extra_vars', ''),
                script_base=script_base,
                **task_kwargs
            )
        elif task_type == 'gsheet':
            raise ValueError(
                f"task_type 'gsheet' is no longer supported. "
                f"Use task_type 'etl' with layout containing type='gsheet' in inputSources/outputs instead."
            )
        elif task_type == 'pipeline':
            task = create_pipeline_operator(
                dag=dag,
                task_id=task_name,
                script_name=config['script'],
                params=config.get('params', []),
                stage=config.get('stage'),
                script_base=script_base,
                **task_kwargs
            )
        else:
            raise ValueError(f"Unknown task type: {task_type}")
        
        tasks[task_name] = task
    
    return tasks
