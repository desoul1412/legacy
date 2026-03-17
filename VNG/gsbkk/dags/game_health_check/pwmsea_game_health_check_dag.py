"""
PWMSEA Game Health Check DAG

Dedicated DAG for Perfect World M SEA (game_id: 425)
Market type: sea (SEA aggregate)
Schema: GDS Postgres common_tables_2 (ops.daily_role_active_details, mkt.daily_user_active)

Pipeline Flow:
1. ETL: Extract transaction-level data from GDS Postgres → HDFS Parquet
2. STD: Aggregate campaign metrics → HDFS Parquet
3. CONS: Consolidate and write to TSN Postgres

Runs daily at 4 AM UTC. Uses logDate = ds - 1 day (previous day's data).

Migrated from: layouts/game_health_check/pwmsea/ + create_pipeline_tasks(etl)
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.dag_helpers import create_sql_operator

# ==============================================================================
# CONSTANTS
# ==============================================================================

HDFS_BASE   = 'hdfs://c0s/user/gsbkk-workspace-yc9t6'
GHC         = HDFS_BASE + '/game_health_check/pwmsea'
CURRENCY    = HDFS_BASE + '/currency_mapping'
TEMPLATES   = 'transform/game_health_check'
USER_PROF   = TEMPLATES + '/pwmsea/user_profile.sql.j2'
LOG_DATE    = '{{ macros.ds_add(ds, -1) }}'

# ==============================================================================
# DAG
# ==============================================================================

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

# ==============================================================================
# ETL — Extract from GDS Postgres → HDFS Parquet
# ==============================================================================

etl_active_details = create_sql_operator(
    dag=dag,
    task_id='etl_active_details',
    extract_sql_file=TEMPLATES + '/pwmsea/active_details.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/active_details/{logDate}',
    log_date=LOG_DATE,
    game_id='pwmsea',
)

etl_charge_details = create_sql_operator(
    dag=dag,
    task_id='etl_charge_details',
    extract_sql_file=TEMPLATES + '/pwmsea/charge_details.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/charge_details/{logDate}',
    log_date=LOG_DATE,
    game_id='pwmsea',
)

etl_campaign = create_sql_operator(
    dag=dag,
    task_id='etl_campaign',
    extract_sql_file=TEMPLATES + '/pwmsea/campaign.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/campaign',
    log_date=LOG_DATE,
    game_id='pwmsea',
)

# ==============================================================================
# STD — Aggregate campaign metrics → HDFS Parquet
# ==============================================================================

std_active = create_sql_operator(
    dag=dag,
    task_id='std_active',
    extract_sql_file=TEMPLATES + '/pwmsea/active.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/std/active/{logDate}',
    log_date=LOG_DATE,
    game_id='pwmsea',
)

std_charge = create_sql_operator(
    dag=dag,
    task_id='std_charge',
    extract_sql_file=TEMPLATES + '/pwmsea/charge.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/std/charge/{logDate}',
    log_date=LOG_DATE,
    game_id='pwmsea',
)

std_retention = create_sql_operator(
    dag=dag,
    task_id='std_retention',
    extract_sql_file=TEMPLATES + '/pwmsea/retention_data.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/std/retention',
    log_date=LOG_DATE,
    game_id='pwmsea',
)

# ==============================================================================
# CONS — Consolidate and write to TSN Postgres
# ==============================================================================

cons_diagnostic_daily = create_sql_operator(
    dag=dag,
    task_id='cons_diagnostic_daily',
    sql_file=TEMPLATES + '/cons/diagnostic_daily.sql.j2',
    input_paths=';'.join([
        'currency_mapping|' + CURRENCY,
        'charge_details|'   + GHC + '/std/charge/{logDate}',
        'active_details|'   + GHC + '/std/active/{logDate}',
        'campaign|'         + GHC + '/campaign',
        'retention_details|'+ GHC + '/std/retention',
    ]),
    secondary_sql_file=USER_PROF,
    secondary_connection='GDS_POSTGRES',
    secondary_view='user_profile',
    output_type='jdbc',
    output_table='public.ghc_diagnostic_daily_pwmsea',
    output_mode='append',
    delete_condition="report_date IN (DATE '{{ logDate }}', DATE '{{ logDate }}' - INTERVAL '1 day')",
    log_date=LOG_DATE,
    game_id='pwmsea',
)

cons_package_performance = create_sql_operator(
    dag=dag,
    task_id='cons_package_performance',
    sql_file=TEMPLATES + '/cons/package_performance.sql.j2',
    input_path=GHC + '/charge_details/{logDate}',
    input_view='charge_details',
    secondary_sql_file=USER_PROF,
    secondary_connection='GDS_POSTGRES',
    secondary_view='user_profile',
    output_type='jdbc',
    output_table='public.ghc_package_performance_pwmsea',
    output_mode='append',
    delete_condition="date = '{{ logDate }}'",
    num_partitions=2,
    log_date=LOG_DATE,
    game_id='pwmsea',
)

cons_server_performance = create_sql_operator(
    dag=dag,
    task_id='cons_server_performance',
    sql_file=TEMPLATES + '/cons/server_performance.sql.j2',
    input_paths=';'.join([
        'active_details|' + GHC + '/active_details/{logDate}',
        'charge_details|' + GHC + '/charge_details/{logDate}',
    ]),
    secondary_sql_file=USER_PROF,
    secondary_connection='GDS_POSTGRES',
    secondary_view='user_profile',
    output_type='jdbc',
    output_table='public.ghc_server_performance_pwmsea',
    output_mode='append',
    delete_condition="date = '{{ logDate }}'",
    num_partitions=2,
    log_date=LOG_DATE,
    game_id='pwmsea',
)

# ==============================================================================
# CONTROL FLOW
# ==============================================================================

start        = EmptyOperator(task_id='start',        dag=dag)
etl_complete = EmptyOperator(task_id='etl_complete', dag=dag)
std_complete = EmptyOperator(task_id='std_complete', dag=dag)
cons_complete= EmptyOperator(task_id='cons_complete', dag=dag)

trigger_rfc = TriggerDagRunOperator(
    task_id='trigger_rolling_forecast',
    trigger_dag_id='pwmsea_daily_actual_rfc',
    conf={'log_date': '{{ ds }}'},
    wait_for_completion=False,
    dag=dag,
)

start >> [etl_active_details, etl_charge_details, etl_campaign] >> etl_complete

etl_complete >> [std_active, std_charge, std_retention] >> std_complete

std_complete >> [
    cons_diagnostic_daily,
    cons_package_performance,
    cons_server_performance,
] >> cons_complete >> trigger_rfc
