"""
Currency Mapping DAG

This DAG extracts currency exchange rates from GDS Postgres to HDFS.
It runs monthly on the 2nd at 1 PM GMT+7 (6 AM UTC).

The currency mapping table is a shared resource used by all pipelines
to convert revenue from local currencies to USD and VND.

Data Flow:
1. Extract ALL historical exchange rates from public.currency_mapping in GDS Postgres
2. Calculate exchange_rate_to_usd (using VND as base)
3. Overwrite HDFS at hdfs://c0s/user/gsbkk-workspace-yc9t6/currency_mapping
4. Consolidation SQLs join by report_month (previous month) with fallback to latest

Schedule: Monthly on 2nd at 1 PM GMT+7 (0 6 2 * *)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'gsbkk',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='currency_mapping_monthly',
    default_args=default_args,
    description='Extract currency exchange rates monthly',
    schedule_interval='0 6 2 * *',  # 2nd of month, 6 AM UTC (1 PM GMT+7)
    start_date=datetime(2025, 1, 2),
    catchup=False,
    tags=['common', 'currency', 'etl'],
) as dag:

    extract_currency = BashOperator(
        task_id='extract_currency_mapping',
        bash_command="""
        cd /opt/airflow/dags/repo && \
        ./run_etl_process.sh \
            --layout layouts/common/currency_mapping.json \
            --logDate {{ ds }}
        """
    )
