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


def create_api_operator(
    dag,
    task_id: str,
    layout: str,
    month: str = "{{ execution_date.strftime('%Y-%m') }}",
    script_base: str = '/opt/airflow/dags/repo',
    **kwargs
) -> BashOperator:
    """
    Create a BashOperator for API extraction tasks using run_api_extraction.sh
    
    Args:
        dag: Airflow DAG object
        task_id: Task identifier
        layout: Path to layout JSON file
        month: Month template (default: {{ execution_date.strftime('%Y-%m') }})
        script_base: Base path to scripts
        **kwargs: Additional BashOperator parameters
    
    Returns:
        BashOperator instance
    """
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            cd {script_base}
            ./run_api_extraction.sh \
                --layout {layout} \
                --month {month}
        """,
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
