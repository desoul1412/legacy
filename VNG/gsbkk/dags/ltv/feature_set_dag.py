import os
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

if AIRFLOW_HOME is None:
    AIRFLOW_HOME = "/usr/local/airflow"
# [END import_module]
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")


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
    tags=['test', 'processor']
)

with TaskGroup(group_id='STD', dag=dag) as std_group:
    # Task STD:
    std_login = BashOperator(
        task_id='std_login',
        bash_command=f"""sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_login.sql hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/trino-jdbc-368.jar""",
        dag=dag
    )

    std_charge = BashOperator(
        task_id='std_charge',
        bash_command=f"""sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_charge.sql hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/trino-jdbc-368.jar""",
        dag=dag
    )

    std_item = BashOperator(
        task_id='std_item',
        bash_command=f"""sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_item.sql hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/trino-jdbc-368.jar""",
        dag=dag
    )

    std_money = BashOperator(
        task_id='std_money',
        bash_command=f"""sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_money.sql hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/trino-jdbc-368.jar""",
        dag=dag
    )

    std_ltv = BashOperator(
        task_id='std_ltv',
        bash_command=f"""sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/std/std_ltv.sql hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/trino-jdbc-368.jar""",
        dag=dag
    )

with TaskGroup(group_id='CONS', dag=dag) as cons_group:
    # TASK CONS:
    feature_set = BashOperator(
        task_id='feature_set',
        bash_command=f"""sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh gnoth 2025-01-01 gnoth/cons/feature_set.sql hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/trino-jdbc-368.jar""",
        dag=dag
    )

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)


# Set task dependencies

start >> std_group >> cons_group >> end
