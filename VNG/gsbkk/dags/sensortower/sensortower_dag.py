"""
SensorTower Market Research Pipeline DAG

Pipeline Flow (SQL-First Architecture):
  RAW Stage  : API extraction → JSON files on HDFS  (run_pipeline.sh / market_research.py)
  ETL Stage  : JSON → Parquet  (run_sql.sh / sql_runner.py + transform/sensortower/etl/)
  STD Stage  : Parquet → Standardized Parquet  (run_sql.sh / sql_runner.py + transform/sensortower/std/)
  CONS Stage : STD → PostgreSQL  (run_sql.sh / sql_runner.py + transform/sensortower/cons/)

RAW Extraction Note:
  SensorTower uses run_pipeline.sh / market_research.py instead of run_api.sh / api_to_raw.py
  because the extraction is too complex for a single HTTP call:
    - Top games:  multi-country parallel fetches (VN, TH, ID, PH, SG, JP, KR, ...)
    - New games:  separate iOS + Android calls, paginated by offset
    - Metadata:   batched 100 app_ids at a time (app_ids come from discovery step)
    - Game tags:  batched alongside metadata
    - Performance: batched by country × app_id, cross-step data dependency

  Use create_raw_api_operator (run_api.sh / api_to_raw.py) for simpler pipelines where
  a single URL + optional pagination covers the full extraction (e.g. Google Sheets,
  single-endpoint REST APIs without cross-step data dependencies).

Author: GSBKK Team
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from utils.dag_helpers import create_pipeline_operator, create_sql_operator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

default_args = {
    'owner': 'gsbkk',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sensortower_market_research',
    default_args=default_args,
    description='SensorTower market research data pipeline',
    schedule_interval='0 2 1 * *',  # 2 AM on 1st of each month
    catchup=False,
    tags=['sensortower', 'market_research', 'api'],
)

# Previous month in YYYY-MM format
month_template = "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m') }}"

HDFS = 'hdfs://c0s/user/gsbkk-workspace-yc9t6/sensortower'
CONS = 'transform/sensortower/cons'

# Control tasks
start                = EmptyOperator(task_id='start', dag=dag)
discovery_complete   = EmptyOperator(task_id='discovery_complete', dag=dag)
app_details_complete = EmptyOperator(task_id='app_details_complete', dag=dag)
end                  = EmptyOperator(task_id='end', dag=dag)

# ---------------------------------------------------------------------------
# Task Group 1: Discovery Phase — extract top/new games, ETL + STD + CONS
# ---------------------------------------------------------------------------
with TaskGroup('discovery_phase', tooltip='Top games & new games: extract → ETL → STD → CONS', dag=dag) as discovery_group:

    # RAW: multi-country parallel fetch via market_research.py
    # (too complex for create_raw_api_operator — see module docstring above)
    extract_top_games = create_pipeline_operator(
        dag=dag,
        task_id='extract_top_games',
        script_name='market_research',
        params=['extract_top_games', month_template],
    )
    extract_new_games = create_pipeline_operator(
        dag=dag,
        task_id='extract_new_games',
        script_name='market_research',
        params=['extract_new_games', month_template],
    )

    # ETL: RAW JSON → Parquet
    etl_top_games = create_sql_operator(
        dag=dag,
        task_id='etl_top_games',
        sql_file='transform/sensortower/etl/top_games.sql.j2',
        input_path=HDFS + '/raw/top_games/*/{logDate}',
        input_format='json',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/etl/top_games/{logDate}',
        output_mode='overwrite',
        log_date=month_template,
    )
    etl_new_games = create_sql_operator(
        dag=dag,
        task_id='etl_new_games',
        sql_file='transform/sensortower/etl/new_games.sql.j2',
        input_path=HDFS + '/raw/new_games/{logDate}',
        input_format='json',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/etl/new_games/{logDate}',
        output_mode='overwrite',
        log_date=month_template,
    )

    # STD: ETL Parquet → standardized Parquet
    std_top_games = create_sql_operator(
        dag=dag,
        task_id='std_top_games',
        sql_file='transform/sensortower/std/top_games.sql.j2',
        input_path=HDFS + '/etl/top_games/{logDate}',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/std/top_games/month={logDate}-01',
        output_mode='overwrite',
        log_date=month_template,
    )
    std_new_games = create_sql_operator(
        dag=dag,
        task_id='std_new_games',
        sql_file='transform/sensortower/std/new_games.sql.j2',
        input_path=HDFS + '/etl/new_games/{logDate}',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/std/new_games/month={logDate}-01',
        output_mode='overwrite',
        log_date=month_template,
    )

    # CONS: UNION top_games + new_games → public.market_insights
    cons_market_insights = create_sql_operator(
        dag=dag,
        task_id='cons_market_insights',
        sql_file=CONS + '/market_insights.sql.j2',
        input_paths=';'.join([
            'top_games|' + HDFS + '/std/top_games/month={logDate}-01',
            'new_games|' + HDFS + '/std/new_games/month={logDate}-01',
        ]),
        output_type='jdbc',
        output_table='public.market_insights',
        output_mode='append',
        log_date=month_template,
    )

    extract_top_games >> etl_top_games >> std_top_games
    extract_new_games >> etl_new_games >> std_new_games
    [std_top_games, std_new_games] >> cons_market_insights

# ---------------------------------------------------------------------------
# Task Group 2: App Details Phase — metadata + game tags, ETL + dimension tables
# ---------------------------------------------------------------------------
with TaskGroup('app_details_phase', tooltip='Metadata & game tags: extract → ETL → dimension tables', dag=dag) as app_details_group:

    # RAW: batch metadata fetch (app_ids sourced from discovery phase output)
    # (batched 100 at a time via market_research.py — not a simple single-URL call)
    extract_metadata = create_pipeline_operator(
        dag=dag,
        task_id='extract_metadata',
        script_name='market_research',
        params=['extract_metadata', month_template],
    )
    extract_game_tags = create_pipeline_operator(
        dag=dag,
        task_id='extract_game_tags',
        script_name='market_research',
        params=['extract_metadata', month_template],
    )

    # ETL: RAW JSON → Parquet
    etl_metadata = create_sql_operator(
        dag=dag,
        task_id='etl_metadata',
        sql_file='transform/sensortower/etl/metadata.sql.j2',
        input_path=HDFS + '/raw/metadata/{logDate}-01',
        input_format='json',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/etl/metadata/{logDate}-01',
        num_partitions=4,
        log_date=month_template,
    )
    etl_game_tags = create_sql_operator(
        dag=dag,
        task_id='etl_game_tags',
        sql_file='transform/sensortower/etl/game_tags.sql.j2',
        input_path=HDFS + '/raw/game_tags/{logDate}-01',
        input_format='json',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/etl/game_tags/{logDate}-01',
        num_partitions=4,
        log_date=month_template,
    )

    # STD: ETL Parquet → standardized Parquet
    std_metadata = create_sql_operator(
        dag=dag,
        task_id='std_metadata',
        sql_file='transform/sensortower/std/metadata.sql.j2',
        input_path=HDFS + '/etl/metadata/{logDate}-01',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/std/metadata/{logDate}-01',
        num_partitions=8,
        log_date=month_template,
    )

    # CONS: build dim_app_variants (metadata + game_tags → test.dim_app_variants)
    cons_dim_app_variants = create_sql_operator(
        dag=dag,
        task_id='cons_dim_app_variants',
        sql_file=CONS + '/dim_app_variants.sql.j2',
        input_paths=';'.join([
            'metadata|'  + HDFS + '/etl/metadata/{logDate}-01',
            'game_tags|' + HDFS + '/etl/game_tags/{logDate}-01',
        ]),
        output_type='jdbc',
        output_table='test.dim_app_variants',
        output_mode='append',
        log_date=month_template,
    )

    # CONS: build dim_apps (all metadata + game_tags + existing dim_apps → test.dim_apps)
    # Uses current_dim_apps as secondary JDBC to insert only new apps (WHERE NOT EXISTS)
    cons_dim_apps = create_sql_operator(
        dag=dag,
        task_id='cons_dim_apps',
        sql_file=CONS + '/dim_apps.sql.j2',
        input_paths=';'.join([
            'metadata|'  + HDFS + '/etl/metadata/*',
            'game_tags|' + HDFS + '/etl/game_tags/*',
        ]),
        secondary_sql_file=CONS + '/current_dim_apps.sql.j2',
        secondary_connection='TSN_POSTGRES',
        secondary_view='current_dim_apps',
        output_type='jdbc',
        output_table='test.dim_apps',
        output_mode='append',
        log_date=month_template,
    )

    etl_complete = EmptyOperator(task_id='etl_complete', dag=dag)

    extract_metadata  >> etl_metadata  >> std_metadata
    extract_game_tags >> etl_game_tags
    [std_metadata, etl_game_tags] >> etl_complete
    etl_complete >> cons_dim_app_variants >> cons_dim_apps

# ---------------------------------------------------------------------------
# Task Group 3: Daily Performance Phase — extract → ETL → STD → CONS
# ---------------------------------------------------------------------------
with TaskGroup('daily_performance_phase', tooltip='Daily performance: extract → ETL → STD → CONS', dag=dag) as performance_group:

    # RAW: batched by country × app_id, requires app_ids from app_details_phase
    # (cross-step data dependency — not suitable for create_raw_api_operator)
    extract_daily_performance = create_pipeline_operator(
        dag=dag,
        task_id='extract_daily_performance',
        script_name='market_research',
        params=['extract_performance', month_template],
    )

    # ETL: RAW JSON → Parquet
    etl_daily_perf = create_sql_operator(
        dag=dag,
        task_id='etl_daily_performance',
        sql_file='transform/sensortower/etl/daily_performance.sql.j2',
        input_path=HDFS + '/raw/daily_performance/month={logDate}-01',
        input_format='json',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/etl/performance/month={logDate}-01',
        output_mode='append',
        log_date=month_template,
    )

    # STD: ETL Parquet → standardized Parquet
    std_daily_perf = create_sql_operator(
        dag=dag,
        task_id='std_daily_performance',
        sql_file='transform/sensortower/std/daily_performance.sql.j2',
        input_path=HDFS + '/etl/performance/month={logDate}-01',
        input_view='source_data',
        output_type='file',
        output_path=HDFS + '/std/performance/month={logDate}-01',
        output_mode='append',
        log_date=month_template,
    )

    # CONS: rolling 3-month performance (STD + dim_app_variants lookup → public.performance_3m)
    cons_daily_performance = create_sql_operator(
        dag=dag,
        task_id='cons_daily_performance',
        sql_file=CONS + '/performance_3m.sql.j2',
        input_path=HDFS + '/std/performance',
        input_view='daily_performance',
        secondary_sql_file=CONS + '/lookup_dim_app_variants.sql.j2',
        secondary_connection='TSN_POSTGRES',
        secondary_view='dim_app_variants',
        output_type='jdbc',
        output_table='public.performance_3m',
        output_mode='overwrite',
        log_date=month_template,
    )

    extract_daily_performance >> etl_daily_perf >> std_daily_perf >> cons_daily_performance

# ---------------------------------------------------------------------------
# Group dependencies
# ---------------------------------------------------------------------------
start >> discovery_group >> discovery_complete
discovery_complete >> app_details_group >> app_details_complete
app_details_complete >> performance_group >> end
