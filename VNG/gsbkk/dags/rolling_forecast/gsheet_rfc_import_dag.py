"""
Rolling Forecast Google Sheets Import DAG

Imports rolling forecast data from Google Sheets to PostgreSQL.

Pipeline Flow (two steps per dataset):
  1. raw_*_rfc  : read Google Sheet → write JSON lines to HDFS RAW layer
  2. cons_*_rfc : read HDFS RAW     → apply SQL transform → write to Postgres

Both daily and monthly datasets run in parallel.

Sheet: https://docs.google.com/spreadsheets/d/1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w

Migrated from: layouts/rolling_forecast/gsheet_daily_rfc.json
               layouts/rolling_forecast/gsheet_monthly_rfc.json
               + etl_engine.py (gsheet input type)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.dag_helpers import create_gsheet_raw_operator, create_sql_operator

# ==============================================================================
# CONSTANTS
# ==============================================================================

SHEET_ID    = '1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w'
HDFS_BASE   = 'hdfs://c0s/user/gsbkk-workspace-yc9t6'
HDFS_RAW    = HDFS_BASE + '/raw/rfc'

# ==============================================================================
# DAG
# ==============================================================================

dag = DAG(
    dag_id='rolling_forecast_gsheet_import',
    default_args={
        'owner': 'baoln2',
        'retries': 1,
        'start_date': datetime(2026, 1, 20),
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
    },
    description='Import rolling forecast data from Google Sheets to PostgreSQL',
    schedule_interval='0 9 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['rolling_forecast', 'gsheet', 'import'],
)

# ==============================================================================
# DAILY RFC
# ==============================================================================

# Step 1: Google Sheet → HDFS RAW
raw_daily_rfc = create_gsheet_raw_operator(
    dag=dag,
    task_id='raw_daily_rfc',
    sheet_id=SHEET_ID,
    worksheet='Daily Overall',
    output_path=HDFS_RAW + '/daily/{logDate}',
)

# Step 2: HDFS RAW → Postgres
cons_daily_rfc = create_sql_operator(
    dag=dag,
    task_id='cons_daily_rfc',
    sql_file='transform/rolling_forecast/cons/rfc_daily.sql.j2',
    output_type='jdbc',
    input_path=HDFS_RAW + '/daily/{logDate}',
    input_view='daily_rfc',
    output_table='public.rfc_daily',
    output_mode='append',
    delete_condition='(date IS NULL OR game IS NULL) OR EXTRACT(YEAR FROM date) = {{ ds[:4] }}',
    game_id='rfc',
)

# ==============================================================================
# MONTHLY RFC
# ==============================================================================

# Step 1: Google Sheet → HDFS RAW
raw_monthly_rfc = create_gsheet_raw_operator(
    dag=dag,
    task_id='raw_monthly_rfc',
    sheet_id=SHEET_ID,
    worksheet='Monthly Overall',
    output_path=HDFS_RAW + '/monthly/{logDate}',
)

# Step 2: HDFS RAW → Postgres
cons_monthly_rfc = create_sql_operator(
    dag=dag,
    task_id='cons_monthly_rfc',
    sql_file='transform/rolling_forecast/cons/rfc_monthly.sql.j2',
    output_type='jdbc',
    input_path=HDFS_RAW + '/monthly/{logDate}',
    input_view='monthly_rfc',
    output_table='public.rfc_monthly',
    output_mode='append',
    delete_condition="(month IS NULL OR game IS NULL) OR SUBSTRING(CAST(month AS VARCHAR), 1, 4) = '{{ ds[:4] }}'",
    game_id='rfc',
)

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

start = EmptyOperator(task_id='start', dag=dag)
end   = EmptyOperator(task_id='end',   dag=dag)

start >> [raw_daily_rfc, raw_monthly_rfc]
raw_daily_rfc   >> cons_daily_rfc   >> end
raw_monthly_rfc >> cons_monthly_rfc >> end
