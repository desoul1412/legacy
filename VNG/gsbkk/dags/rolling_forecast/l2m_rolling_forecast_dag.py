"""
L2M Rolling Forecast DAG

Creates separate DAGs for each country (TH, ID, PH, etc.)
Each country writes to its own Google Sheet.

Pipeline Flow:
1. CONS: Read diagnostic_daily → Filter by country → public.rfc_daily_l2m → Country-specific GSheet

Execution: Triggered by game_health_check_l2m DAG after CONS tasks complete
Market type: multi (per-country)
Schema: TSN Postgres (public.ghc_diagnostic_daily_l2m)
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.dag_helpers import create_pipeline_tasks
import yaml

# Load L2M game configuration
with open('/opt/airflow/dags/repo/configs/game_configs/game_name.yaml', 'r') as f:
    GAME_CONFIGS = yaml.safe_load(f)['games']
    L2M_CONFIG = GAME_CONFIGS.get('l2m', {})

# Create one DAG per country
countries = L2M_CONFIG.get('countries', [])
for country_config in countries:
    country_code = list(country_config.keys())[0]
    country_data = country_config[country_code]
    rfc_sheet_id = country_data.get('rfc_sheet_id', '')
    game_name = country_data.get('game_name', f'L2M - {country_code}')
    
    # DAG configuration
    dag = DAG(
        dag_id=f"l2m_{country_code}_daily_actual_rfc",
        default_args={
            "owner": "trangnm10",
            "retries": 1,
            "start_date": datetime(2025, 1, 6),
            "retry_delay": timedelta(minutes=5),
            "depends_on_past": False,
        },
        description=f"Daily rolling forecast for {game_name}",
        schedule_interval=None,  # Triggered by game_health_check_l2m DAG
        catchup=False,
        max_active_runs=1,
        tags=["rolling_forecast", "l2m", country_code, "multi"],
    )
    
    # Pipeline: Single CONS step - reads from diagnostic_daily table
    pipeline = [
        {'name': 'cons_metrics', 'type': 'etl', 
         'layout': 'layouts/rolling_forecast/l2m/cons/metrics_gsheet.json',
         'vars': f'market_type=multi,gameId=l2m,countryCode={country_code},rfcSheetId={rfc_sheet_id}'},
    ]
    
    # Use log_date from trigger conf if provided, otherwise use ds (data_interval_start)
    log_date = "{{ dag_run.conf.get('log_date', ds) }}"
    tasks = create_pipeline_tasks(dag, pipeline, game_id='l2m', log_date=log_date)
    
    # Control flow
    start = EmptyOperator(task_id='start', dag=dag)
    end = EmptyOperator(task_id='end', dag=dag)
    
    # Dependencies: cons_metrics writes to both Postgres and country-specific GSheet
    start >> tasks['cons_metrics'] >> end
    
    # Register DAG in globals
    globals()[f"l2m_{country_code}_daily_actual_rfc"] = dag
