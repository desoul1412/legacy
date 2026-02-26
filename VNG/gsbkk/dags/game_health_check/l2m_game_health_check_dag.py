"""
L2M Game Health Check DAG

Dedicated DAG for L2M game health monitoring with Trino data sources.
Market type: multi (per-country)
Schema: Mixed - Trino (iceberg.l2m.etl_login/etl_recharge) + GDS Postgres (public.mkt_user_active)

Pipeline Flow:
1. ETL: Extract from Trino (active/charge) and GDS Postgres (retention/campaign)
2. STD: Aggregate campaign metrics
3. CONS: Consolidate and write to TSN Postgres

Runs daily at 3 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.dag_helpers import create_pipeline_tasks
import yaml

# Load L2M game configuration
with open('/opt/airflow/dags/repo/configs/game_configs/game_name.yaml', 'r') as f:
    GAME_CONFIGS = yaml.safe_load(f)['games']


# L2M-specific pipeline configuration
dag = DAG(
    dag_id='game_health_check_l2m',
    default_args={
        'owner': 'gsbkk',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
    },
    description='L2M game health monitoring with Trino sources',
    schedule_interval='0 3 * * *',  # 3 AM daily
    catchup=True,
    max_active_runs=1,
    tags=['game_health', 'l2m', 'monitoring', 'trino'],
)

# Pipeline configuration - L2M-specific layouts with Trino for active/charge details
# L2M game health monitoring - dedicated DAG
pipeline = [
    # ETL Stage: Extract transaction-level data
    # L2M uses GDS_TRINO connection for iceberg tables
    {'name': 'etl_active_details', 'type': 'etl', 
     'layout': 'layouts/game_health_check/l2m/etl/active_details.json'},
    {'name': 'etl_charge_details', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/etl/charge_details.json'},
    {'name': 'etl_campaign', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/etl/campaign.json'},
    
    # STD Stage: Aggregate campaign metrics for diagnostic analysis
    {'name': 'std_active', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/std/active.json'},
    {'name': 'std_charge', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/std/charge.json'},
    {'name': 'std_retention', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/std/retention.json'},
    
    # CONS Stage: Consolidate metrics (user_profile read directly from GDS Postgres)
    {'name': 'cons_diagnostic_daily', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/cons/diagnostic_daily.json'},
    {'name': 'cons_package_performance', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/cons/package_performance.json'},
    {'name': 'cons_server_performance', 'type': 'etl',
     'layout': 'layouts/game_health_check/l2m/cons/server_performance.json'},
]

# Create all tasks from configuration
tasks = create_pipeline_tasks(dag, pipeline, game_id='l2m', log_date='{{ ds }}')

# Control flow tasks
start = EmptyOperator(task_id='start', dag=dag)
etl_complete = EmptyOperator(task_id='etl_complete', dag=dag)
std_complete = EmptyOperator(task_id='std_complete', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# ==================== DEPENDENCIES ====================
# ETL tasks (transaction-level) run in parallel
# → STD tasks (campaign aggregation) run in parallel
# → CONS tasks run in parallel

start >> [
    tasks['etl_active_details'],
    tasks['etl_charge_details'],
    tasks['etl_campaign']
] >> etl_complete

etl_complete >> [
    tasks['std_active'],
    tasks['std_charge'],
    tasks['std_retention']
] >> std_complete

std_complete >> [
    tasks['cons_diagnostic_daily'],
    tasks['cons_package_performance'],
    tasks['cons_server_performance']
] >> end

# ==================== ROLLING FORECAST TRIGGERS ====================
# Auto-trigger L2M rolling forecast DAGs (per country) after game health check completes

game_config = GAME_CONFIGS.get('l2m', {})
countries = game_config.get('countries', [])

trigger_tasks = []
for country_config in countries:
    country_code = list(country_config.keys())[0]
    
    trigger_rfc = TriggerDagRunOperator(
        task_id=f'trigger_rfc_{country_code}',
        trigger_dag_id=f'l2m_{country_code}_daily_actual_rfc',
        conf={'log_date': '{{ ds }}'},
        wait_for_completion=False,
        reset_dag_run=True,  # Reset if already exists
        dag=dag
    )
    trigger_tasks.append(trigger_rfc)

# All country RFC DAGs triggered in parallel after game health check ends
if trigger_tasks:
    end >> trigger_tasks
