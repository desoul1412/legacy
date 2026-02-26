"""
GNOTH Rolling Forecast DAG

Dedicated DAG for Gunny Origin TH (game_id: 496)
Market type: single (TH only)
Schema: TSN Postgres (reads from ghc_diagnostic_daily_gnoth)

Pipeline Flow:
1. CONS: Read diagnostic_daily table → public.rfc_daily_gnoth + Google Sheet

Execution: Triggered by game_health_check_gnoth DAG after diagnostic complete
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.dag_helpers import create_pipeline_tasks

dag = DAG(
    dag_id="gnoth_daily_actual_rfc",
    default_args={
        "owner": "trangnm10",
        "retries": 1,
        "start_date": datetime(2025, 1, 6),
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
    },
    description="Daily rolling forecast for GNOTH (496 - Gunny Origin - TH)",
    schedule_interval=None,  # Triggered by game_health_check_gnoth DAG
    catchup=False,
    max_active_runs=1,
    tags=["rolling_forecast", "gnoth", "single", "th"],
)

# Pipeline configuration
# Single CONS step - reads all metrics from diagnostic_daily table
pipeline = [
    {
        'name': 'cons_metrics',
        'type': 'etl',
        'layout': 'layouts/rolling_forecast/gnoth/cons/metrics_gsheet.json',
        'vars': 'market_type=single,gameId=gnoth'
    },
]

# Use log_date from trigger conf if provided, otherwise use ds (data_interval_start)
log_date = "{{ dag_run.conf.get('log_date', ds) }}"
tasks = create_pipeline_tasks(dag, pipeline, game_id='gnoth', log_date=log_date)

# Task dependencies
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

start >> tasks['cons_metrics'] >> end
