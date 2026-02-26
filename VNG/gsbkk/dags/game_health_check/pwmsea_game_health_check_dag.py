"""
PWMSEA Game Health Check DAG

Dedicated DAG for Perfect World M SEA (game_id: 425)
Market type: sea (SEA aggregate)
Schema: GDS Postgres common_tables_2 (ops.daily_role_active_details, mkt.daily_user_active)

Pipeline Flow:
1. ETL: Extract transaction-level data from GDS Postgres → HDFS Parquet
2. STD: Aggregate campaign metrics → HDFS Parquet
3. CONS: Consolidate and write to TSN Postgres

Runs daily at 6 AM UTC (1 PM GMT+7)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.dag_helpers import create_pipeline_tasks

dag = DAG(
    dag_id='game_health_check_pwmsea',
    default_args={
        'owner': 'gsbkk',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
    },
    description='PWMSEA game health monitoring - dedicated DAG',
    schedule_interval='0 4 * * *',
    catchup=True,
    max_active_runs=1,
    tags=['game_health', 'pwmsea', 'monitoring'],
)

# Pipeline configuration - PWMSEA-specific layouts
pipeline = [
    # ETL Stage
    {'name': 'etl_active_details', 'type': 'etl', 
     'layout': 'layouts/game_health_check/pwmsea/etl/active_details.json'},
    {'name': 'etl_charge_details', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/etl/charge_details.json'},
    {'name': 'etl_campaign', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/etl/campaign.json'},
    
    # STD Stage
    {'name': 'std_active', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/std/active.json'},
    {'name': 'std_charge', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/std/charge.json'},
    {'name': 'std_retention', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/std/retention.json'},
    
    # CONS Stage
    {'name': 'cons_diagnostic_daily', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/cons/diagnostic_daily.json'},
    {'name': 'cons_package_performance', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/cons/package_performance.json'},
    {'name': 'cons_server_performance', 'type': 'etl',
     'layout': 'layouts/game_health_check/pwmsea/cons/server_performance.json'},
]

tasks = create_pipeline_tasks(dag, pipeline, game_id='pwmsea', log_date='{{ macros.ds_add(ds, -1) }}')

# Control flow
start = EmptyOperator(task_id='start', dag=dag)
etl_complete = EmptyOperator(task_id='etl_complete', dag=dag)
std_complete = EmptyOperator(task_id='std_complete', dag=dag)
cons_complete = EmptyOperator(task_id='cons_complete', dag=dag)

# Trigger rolling forecast DAG after diagnostic complete
trigger_rfc = TriggerDagRunOperator(
    task_id='trigger_rolling_forecast',
    trigger_dag_id='pwmsea_daily_actual_rfc',
    conf={'log_date': '{{ ds }}'},
    wait_for_completion=False,
    dag=dag,
)

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
] >> cons_complete >> trigger_rfc

cons_complete >> trigger_rfc

