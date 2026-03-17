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

Migrated from: layouts/game_health_check/l2m/ + create_pipeline_tasks(etl)
"""

import sys
import os
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.dag_helpers import create_sql_operator

# Load L2M game configuration (for per-country RFC triggers)
with open('/opt/airflow/dags/repo/configs/game_configs/game_name.yaml', 'r') as f:
    GAME_CONFIGS = yaml.safe_load(f)['games']

# ==============================================================================
# CONSTANTS
# ==============================================================================

HDFS_BASE   = 'hdfs://c0s/user/gsbkk-workspace-yc9t6'
GHC         = HDFS_BASE + '/game_health_check/l2m'
CURRENCY    = HDFS_BASE + '/currency_mapping'
TEMPLATES   = 'transform/game_health_check'
USER_PROF   = TEMPLATES + '/l2m/user_profile.sql.j2'

# ==============================================================================
# DAG
# ==============================================================================

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
    schedule_interval='0 3 * * *',
    catchup=True,
    max_active_runs=1,
    tags=['game_health', 'l2m', 'monitoring', 'trino'],
)

# ==============================================================================
# ETL — Extract from Trino (active/charge) + GDS Postgres (campaign) → HDFS
# ==============================================================================

etl_active_details = create_sql_operator(
    dag=dag,
    task_id='etl_active_details',
    extract_sql_file=TEMPLATES + '/l2m/active_details.sql.j2',
    extract_connection='GDS_TRINO',
    output_type='file',
    output_path=GHC + '/active_details/{logDate}',
    game_id='l2m',
)

etl_charge_details = create_sql_operator(
    dag=dag,
    task_id='etl_charge_details',
    extract_sql_file=TEMPLATES + '/l2m/charge_details.sql.j2',
    extract_connection='GDS_TRINO',
    output_type='file',
    output_path=GHC + '/charge_details/{logDate}',
    game_id='l2m',
)

etl_campaign = create_sql_operator(
    dag=dag,
    task_id='etl_campaign',
    extract_sql_file=TEMPLATES + '/l2m/campaign.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/campaign',
    game_id='l2m',
)

# ==============================================================================
# STD — Aggregate campaign metrics → HDFS Parquet
# ==============================================================================

std_active = create_sql_operator(
    dag=dag,
    task_id='std_active',
    extract_sql_file=TEMPLATES + '/l2m/active.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/std/active/{logDate}',
    game_id='l2m',
)

std_charge = create_sql_operator(
    dag=dag,
    task_id='std_charge',
    extract_sql_file=TEMPLATES + '/l2m/charge.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/std/charge/{logDate}',
    game_id='l2m',
)

std_retention = create_sql_operator(
    dag=dag,
    task_id='std_retention',
    extract_sql_file=TEMPLATES + '/l2m/retention_data.sql.j2',
    extract_connection='GDS_POSTGRES',
    output_type='file',
    output_path=GHC + '/std/retention',
    game_id='l2m',
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
    output_table='public.ghc_diagnostic_daily_l2m',
    output_mode='append',
    delete_condition="report_date IN (DATE '{{ logDate }}', DATE '{{ logDate }}' - INTERVAL '1 day')",
    game_id='l2m',
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
    output_table='public.ghc_package_performance_l2m',
    output_mode='append',
    delete_condition="date = '{{ logDate }}'",
    num_partitions=2,
    game_id='l2m',
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
    output_table='public.ghc_server_performance_l2m',
    output_mode='append',
    delete_condition="date = '{{ logDate }}'",
    num_partitions=2,
    game_id='l2m',
)

# ==============================================================================
# CONTROL FLOW
# ==============================================================================

start        = EmptyOperator(task_id='start',        dag=dag)
etl_complete = EmptyOperator(task_id='etl_complete', dag=dag)
std_complete = EmptyOperator(task_id='std_complete', dag=dag)
end          = EmptyOperator(task_id='end',          dag=dag)

start >> [etl_active_details, etl_charge_details, etl_campaign] >> etl_complete

etl_complete >> [std_active, std_charge, std_retention] >> std_complete

std_complete >> [
    cons_diagnostic_daily,
    cons_package_performance,
    cons_server_performance,
] >> end

# Trigger per-country RFC DAGs after game health check completes
game_config = GAME_CONFIGS.get('l2m', {})
countries   = game_config.get('countries', [])

trigger_tasks = []
for country_config in countries:
    country_code = list(country_config.keys())[0]
    trigger_tasks.append(TriggerDagRunOperator(
        task_id='trigger_rfc_{0}'.format(country_code),
        trigger_dag_id='l2m_{0}_daily_actual_rfc'.format(country_code),
        conf={'log_date': '{{ ds }}'},
        wait_for_completion=False,
        reset_dag_run=True,
        dag=dag,
    ))

if trigger_tasks:
    end >> trigger_tasks
