"""
SensorTower Market Research Pipeline DAG

This DAG extracts market research data from SensorTower API and processes it
through raw extraction, ETL transformation, and standardization stages.

Pipeline Flow:
  RAW Stage: API extraction → JSON files (top games, metadata, performance)
  ETL Stage: JSON → Parquet conversion
  STD Stage: Parquet → PostgreSQL standardized tables

The extraction is split into modular tasks to prevent entire pipeline failure
if one API endpoint fails.

Author: GSBKK Team
Date: 2025-12-25
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from utils.dag_helpers import create_pipeline_tasks

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Default arguments
default_args = {
    'owner': 'gsbkk',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,  # Increased retries for API calls
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sensortower_market_research',
    default_args=default_args,
    description='SensorTower market research data pipeline',
    schedule_interval='0 2 1 * *',  # Run at 2 AM on 1st of each month
    catchup=False,
    tags=['sensortower', 'market_research', 'api'],
)

# Date template (previous month in YYYY-MM format)
month_template = "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m') }}"

# Define pipeline configuration - step by step for easier debugging
discovery_phase_pipeline = [
    # Step 1: Discovery phase (parallel API calls) - use 'pipeline' type for market_research.py
    {'name': 'extract_top_games', 'type': 'pipeline',
     'script': 'market_research', 'params': ['extract_top_games', month_template]},
    {'name': 'extract_new_games', 'type': 'pipeline',
     'script': 'market_research', 'params': ['extract_new_games', month_template]},

    # Step 2: RAW -> ETL (parallel)
    {'name': 'etl_top_games', 'type': 'etl',
     'layout': 'layouts/sensortower/etl/top_games.json', 'log_date': month_template},
    {'name': 'etl_new_games', 'type': 'etl',
     'layout': 'layouts/sensortower/etl/new_games.json', 'log_date': month_template},

    # Step 3: ETL -> STD (parallel)
    {'name': 'std_top_games', 'type': 'etl',
     'layout': 'layouts/sensortower/std/top_games.json', 'log_date': month_template},
    {'name': 'std_new_games', 'type': 'etl',
     'layout': 'layouts/sensortower/std/new_games.json', 'log_date': month_template},

    # Step 4: STD -> CONS (consolidation)
    {'name': 'market_insights', 'type': 'etl',
     'layout': 'layouts/sensortower/cons/market_insights.json', 'log_date': month_template, 'database': 'sensortower'}
]

app_details_pipeline = [
    # Step 5: Extract metadata (after discovery, get app_ids from CONS) - use 'pipeline' type
    {'name': 'extract_metadata', 'type': 'pipeline',
     'script': 'market_research', 'params': ['extract_metadata', month_template]},
    {'name': 'extract_game_tags', 'type': 'pipeline',
     'script': 'market_research', 'params': ['extract_metadata', month_template]},

    {'name': 'etl_metadata', 'type': 'etl',
     'layout': 'layouts/sensortower/etl/metadata.json', 'log_date': month_template},
    {'name': 'etl_game_tags', 'type': 'etl',
     'layout': 'layouts/sensortower/etl/game_tags.json', 'log_date': month_template},

    # Step 6: RAW -> MF (parallel)
    {'name': 'dim_apps', 'type': 'etl',
     'layout': 'layouts/sensortower/mf/dim_apps.json', 'log_date': month_template},
    {'name': 'dim_app_variants', 'type': 'etl',
     'layout': 'layouts/sensortower/mf/dim_app_variants.json', 'log_date': month_template},
]

daily_performance_pipeline = [
    # Step 7: Get performance of games that haven't had enough of their 1st 90 days performance - use 'pipeline' type
    {'name': 'extract_daily_performance', 'type': 'pipeline',
     'script': 'market_research', 'params': ['extract_performance', month_template]},

    # Step 8: RAW -> ETL -> STD -> CONS
    {'name': 'etl_daily_performance', 'type': 'etl',
     'layout': 'layouts/sensortower/etl/daily_performance.json', 'log_date': month_template},

    {'name': 'std_daily_performance', 'type': 'etl',
     'layout': 'layouts/sensortower/std/daily_performance.json', 'log_date': month_template},

    {'name': 'daily_performance', 'type': 'etl',
     'layout': 'layouts/sensortower/cons/daily_performance.json', 'log_date': month_template},
]

# Create all tasks from configuration
# Control flow tasks
start = EmptyOperator(task_id='start', dag=dag)
discovery_complete = EmptyOperator(task_id='discovery_complete', dag=dag)
app_details_complete = EmptyOperator(task_id='app_details_complete', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# ==================== TASK GROUPS ====================

# Task Group 1: Discovery Phase
with TaskGroup('discovery_phase', tooltip='Extract and process top games & new games to CONS layer', dag=dag) as discovery_group:
    # Create tasks INSIDE the group
    discovery_tasks = create_pipeline_tasks(dag, discovery_phase_pipeline)

    # Set internal dependencies
    discovery_tasks['extract_top_games'] >> discovery_tasks['etl_top_games'] >> discovery_tasks['std_top_games']
    discovery_tasks['extract_new_games'] >> discovery_tasks['etl_new_games'] >> discovery_tasks['std_new_games']
    [discovery_tasks['std_top_games'], discovery_tasks['std_new_games']
     ] >> discovery_tasks['market_insights']

# Task Group 2: App Details Phase
with TaskGroup('app_details_phase', tooltip='Extract metadata & game tags, create dimension tables', dag=dag) as app_details_group:
    # Create tasks INSIDE the group
    app_tasks = create_pipeline_tasks(dag, app_details_pipeline)

    # Set internal dependencies
    app_tasks['extract_metadata'] >> app_tasks['etl_metadata']
    app_tasks['extract_game_tags'] >> app_tasks['etl_game_tags']

    etl_app_details_complete = EmptyOperator(task_id='etl_complete', dag=dag)
    [app_tasks['etl_metadata'], app_tasks['etl_game_tags']] >> etl_app_details_complete
    etl_app_details_complete >> app_tasks['dim_apps'] >> app_tasks['dim_app_variants']

# Task Group 3: Daily Performance Phase
with TaskGroup('daily_performance_phase', tooltip='Extract and process daily performance metrics', dag=dag) as performance_group:
    # Create tasks INSIDE the group
    perf_tasks = create_pipeline_tasks(dag, daily_performance_pipeline)

    # Set internal dependencies
    perf_tasks['extract_daily_performance'] >> perf_tasks['etl_daily_performance'] >> perf_tasks['std_daily_performance'] >> perf_tasks['daily_performance']

# ==================== DEPENDENCIES BETWEEN GROUPS ====================

start >> discovery_group >> discovery_complete
discovery_complete >> app_details_group >> app_details_complete
app_details_complete >> performance_group >> end
