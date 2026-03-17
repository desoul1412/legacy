"""
LTV Feature Set DAG

Builds a per-user feature matrix for LTV modelling over the D0-D7 cohort window.

Pipeline Flow:
  STD (gnoth)  : 5 legacy spark-runner tasks that produce gnoth std views
  LTV_STD      : 6 new modular tasks producing per-feature-group parquet files
  CONS         : 1 final join producing the complete feature set

Migrated from: templates/ltv/feature_set.sql.j2 (400+ line monolith)
"""

import os
import sys
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.dag_helpers import create_sql_operator

# ==============================================================================
# CONSTANTS
# ==============================================================================

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')
HDFS_BASE    = 'hdfs://c0s/user/gsbkk-workspace-yc9t6'
GNOTH_STD    = HDFS_BASE + '/gnoth/std'
LTV_STD      = HDFS_BASE + '/gnoth/ltv/std'
JDBC_JAR     = HDFS_BASE + '/configs/trino-jdbc-368.jar'

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

# ==============================================================================
# DAG
# ==============================================================================

default_args = {
    'owner': 'trangnm10',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'ltv_feature_set',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['ltv', 'processor'],
)

# ==============================================================================
# STD GROUP — legacy gnoth std tasks (unchanged)
# ==============================================================================

with TaskGroup(group_id='STD', dag=dag) as std_group:
    std_login = BashOperator(
        task_id='std_login',
        bash_command=f'sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_login.sql {JDBC_JAR}',
        dag=dag,
    )
    std_charge = BashOperator(
        task_id='std_charge',
        bash_command=f'sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_charge.sql {JDBC_JAR}',
        dag=dag,
    )
    std_item = BashOperator(
        task_id='std_item',
        bash_command=f'sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_item.sql {JDBC_JAR}',
        dag=dag,
    )
    std_money = BashOperator(
        task_id='std_money',
        bash_command=f'sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_money.sql {JDBC_JAR}',
        dag=dag,
    )
    std_ltv = BashOperator(
        task_id='std_ltv',
        bash_command=f'sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_ltv.sql {JDBC_JAR}',
        dag=dag,
    )

# ==============================================================================
# LTV_STD GROUP — modular feature files (new)
# ==============================================================================

_active_base  = 'active|' + GNOTH_STD + '/active/{logDate};base_users|' + GNOTH_STD + '/base_users/{logDate}'
_charge_base  = 'charge|' + GNOTH_STD + '/charge/{logDate};base_users|' + GNOTH_STD + '/base_users/{logDate}'
_item_base    = 'item|'   + GNOTH_STD + '/item/{logDate};base_users|'   + GNOTH_STD + '/base_users/{logDate}'
_money_base   = 'money|'  + GNOTH_STD + '/money/{logDate};base_users|'  + GNOTH_STD + '/base_users/{logDate}'

with TaskGroup(group_id='LTV_STD', dag=dag) as ltv_std_group:
    ltv_sessions = create_sql_operator(
        dag=dag,
        task_id='ltv_sessions',
        sql_file='transform/ltv/std/sessions.sql.j2',
        output_type='file',
        input_paths=_active_base,
        output_path=LTV_STD + '/sessions/{logDate}',
    )
    ltv_retention = create_sql_operator(
        dag=dag,
        task_id='ltv_retention',
        sql_file='transform/ltv/std/retention.sql.j2',
        output_type='file',
        input_paths=_active_base,
        output_path=LTV_STD + '/retention/{logDate}',
    )
    ltv_purchase = create_sql_operator(
        dag=dag,
        task_id='ltv_purchase',
        sql_file='transform/ltv/std/purchase.sql.j2',
        output_type='file',
        input_paths=_charge_base,
        output_path=LTV_STD + '/purchase/{logDate}',
    )
    ltv_items = create_sql_operator(
        dag=dag,
        task_id='ltv_items',
        sql_file='transform/ltv/std/items.sql.j2',
        output_type='file',
        input_paths=_item_base,
        output_path=LTV_STD + '/items/{logDate}',
    )
    ltv_money = create_sql_operator(
        dag=dag,
        task_id='ltv_money',
        sql_file='transform/ltv/std/money.sql.j2',
        output_type='file',
        input_paths=_money_base,
        output_path=LTV_STD + '/money/{logDate}',
    )
    ltv_targets = create_sql_operator(
        dag=dag,
        task_id='ltv_targets',
        sql_file='transform/ltv/std/targets.sql.j2',
        output_type='file',
        input_paths=_charge_base,
        output_path=LTV_STD + '/targets/{logDate}',
    )

# ==============================================================================
# CONS GROUP — final feature join (new)
# ==============================================================================

_cons_inputs = ';'.join([
    'base_users|' + GNOTH_STD  + '/base_users/{logDate}',
    'sessions|'   + LTV_STD    + '/sessions/{logDate}',
    'retention|'  + LTV_STD    + '/retention/{logDate}',
    'purchase|'   + LTV_STD    + '/purchase/{logDate}',
    'items|'      + LTV_STD    + '/items/{logDate}',
    'money|'      + LTV_STD    + '/money/{logDate}',
    'targets|'    + LTV_STD    + '/targets/{logDate}',
])

with TaskGroup(group_id='CONS', dag=dag) as cons_group:
    feature_set = create_sql_operator(
        dag=dag,
        task_id='feature_set',
        sql_file='transform/ltv/cons/feature_set.sql.j2',
        output_type='file',
        input_paths=_cons_inputs,
        output_path=HDFS_BASE + '/gnoth/ltv/cons/feature_set/{logDate}',
    )

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

start = EmptyOperator(task_id='start', dag=dag)
end   = EmptyOperator(task_id='end',   dag=dag)

start >> std_group >> ltv_std_group >> cons_group >> end
