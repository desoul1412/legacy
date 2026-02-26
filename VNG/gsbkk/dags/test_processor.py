import os
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
 
if AIRFLOW_HOME is None:
    AIRFLOW_HOME = "/usr/local/airflow"
# [END import_module]
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")


default_args = {
    'owner': 'baoln2',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'test_processor',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'processor']
)

# Task 1: Show current git commit
test_processor = BashOperator(
    task_id='test_processor',
    bash_command=f"""sh {AIRFLOW_HOME}/dags/repo/spark-runner.sh mts 2025-01-01 mts/login_details.sql hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/trino-jdbc-368.jar""",
    dag=dag
)

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)


# Set task dependencies
start >> test_processor >> end
