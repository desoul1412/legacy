"""
Rolling Forecast Google Sheets Import DAG

Imports rolling forecast data from Google Sheets to PostgreSQL
- Daily RFC data: 'Daily Overall' sheet → public.rfc_daily
- Monthly RFC data: 'Monthly Overall' sheet → public.rfc_monthly

Pipeline Flow:
1. Import daily RFC data from Google Sheets
2. Import monthly RFC data from Google Sheets (runs in parallel)

Execution: Manual trigger or scheduled
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.dag_helpers import create_etl_operator

dag = DAG(
    dag_id="rolling_forecast_gsheet_import_ncv",
    default_args={
        "owner": "sonph4",
        "retries": 1,
        "start_date": datetime(2026, 1, 20),
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
    },
    description="Import rolling forecast data from Google Sheets to PostgreSQL",
    schedule_interval="0 9 * * *",  # Daily at 9 AM
    catchup=False,
    max_active_runs=1,
    tags=["rolling_forecast", "gsheet", "import"],
)

# Task: Import daily RFC data
# Uses ETL operator which calls run_etl_process.sh
import_daily_rfc = create_etl_operator(
    dag=dag,
    task_id='import_daily_rfc_ncv',
    layout='layouts/rolling_forecast/gsheet_daily_rfc_ncv.json',
    game_id='rfc',
    log_date="{{ ds }}"
)

# Task: Import monthly RFC data
# Uses ETL operator which calls run_etl_process.sh
import_monthly_rfc = create_etl_operator(
    dag=dag,
    task_id='import_monthly_rfc_ncv',
    layout='layouts/rolling_forecast/gsheet_monthly_rfc_ncv.json',
    game_id='rfc',
    log_date="{{ ds }}"
)

# Task dependencies
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# Both imports can run in parallel
start >> [import_daily_rfc, import_monthly_rfc] >> end