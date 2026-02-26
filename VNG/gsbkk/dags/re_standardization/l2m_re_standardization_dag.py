"""
L2M Re-Standardization DAG (Complete Pipeline)

This DAG performs comprehensive re-standardization for L2M game data.
Processes raw game logs through ETL → STD → CONS stages to create all business metrics.

Pipeline Flow:
  ETL Stage (7 parallel jobs): Raw logs → HDFS Parquet (active login/logout, recharge, item gain/spend, money gain/spend)
  STD Stage (1 job): HDFS → HDFS union (active user consolidation from 7 sources)
  CONS Stage (11 jobs):
    - Date-dependent (9 parallel): Active, recharge, cumulative revenue, item gain/spend, money gain/spend, server/farmers performance
    - Snapshots (2): Guild info, farmers detection

Focus: Complete L2M data ecosystem - users, revenue, economy, server performance, anti-fraud

Author: GSBKK Team
Date: 2025-12-30
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.dag_helpers import create_pipeline_tasks

# Default arguments
default_args = {
    'owner': 'gsbkk',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}


def create_l2m_dag(game_id='l2m'):
    """
    Create comprehensive L2M re-standardization DAG using declarative pipeline config
    
    Pipeline breakdown:
    - 7 ETL jobs (parallel): Extract from Trino Iceberg to HDFS
    - 1 STD job: Union 7 sources to create active user table
    - 9 CONS jobs (parallel): Core business metrics
    - 2 CONS snapshot jobs: Dimension tables (guild, farmers)
    
    Args:
        game_id: Game identifier (default: 'l2m')
    """
    
    dag = DAG(
        f'l2m_re_standardization',
        default_args=default_args,
        description='L2M complete re-standardization pipeline - users, revenue, economy, anti-fraud',
        schedule_interval='0 5 * * *',  # Run at 5 AM daily
        catchup=False,
        tags=['l2m', 're-standardization', 'complete', 'etl'],
    )
    
    # ==================== LAYER 1: ETL - Extract from Trino to HDFS ====================
    # Define pipeline configuration - 7 parallel extraction jobs
    etl_pipeline = [
        {'name': 'etl_active_login', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/etl/{game_id}_active_login.json'},
        
        {'name': 'etl_active_logout', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/etl/{game_id}_active_logout.json'},
        
        {'name': 'etl_recharge', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/etl/{game_id}_recharge.json'},
        
        {'name': 'etl_item_gain', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/etl/{game_id}_item_gain.json'},
        
        {'name': 'etl_item_spend', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/etl/{game_id}_item_spend.json'},
        
        {'name': 'etl_money_gain', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/etl/{game_id}_money_gain.json'},
        
        {'name': 'etl_money_spend', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/etl/{game_id}_money_spend.json'},
    ]
    
    # ==================== LAYER 2: STD - Union active sources ====================
    std_pipeline = [
        {'name': 'std_active', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/std/{game_id}_active.json'},
    ]
    
    # ==================== LAYER 3: CONS - Business metrics ====================
    # Date-dependent consolidation jobs (run in parallel after STD)
    cons_date_pipeline = [
        {'name': 'cons_active', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_active.json'},
        
        {'name': 'cons_recharge', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_recharge.json'},
        
        {'name': 'cons_cumulative_revenue', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_cumulative_revenue.json'},
        
        {'name': 'cons_item_gain', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_item_gain.json'},
        
        {'name': 'cons_item_spend', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_item_spend.json'},
        
        {'name': 'cons_money_gain', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_money_gain.json'},
        
        {'name': 'cons_money_spend', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_money_spend.json'},
        
        {'name': 'cons_server_performance', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_server_performance.json'},
        
        {'name': 'cons_farmers_performance', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_farmers_performance.json'},
    ]
    
    # Snapshot jobs (dimension tables - run independently)
    cons_snapshot_pipeline = [
        {'name': 'cons_guild_info', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_guild_info.json'},
        
        {'name': 'cons_farmers_snapshot', 'type': 'etl',
         'layout': f'layouts/re-standardization/{game_id}/cons/{game_id}_farmers_snapshot.json'},
    ]
    
    # Create all tasks from configurations
    etl_tasks = create_pipeline_tasks(dag, etl_pipeline, game_id=game_id)
    std_tasks = create_pipeline_tasks(dag, std_pipeline, game_id=game_id)
    cons_date_tasks = create_pipeline_tasks(dag, cons_date_pipeline, game_id=game_id)
    cons_snapshot_tasks = create_pipeline_tasks(dag, cons_snapshot_pipeline, game_id=game_id)
    
    # Control flow tasks
    start = EmptyOperator(task_id='start', dag=dag)
    etl_complete = EmptyOperator(task_id='etl_complete', dag=dag)
    std_complete = EmptyOperator(task_id='std_complete', dag=dag)
    cons_date_complete = EmptyOperator(task_id='cons_date_complete', dag=dag)
    end = EmptyOperator(task_id='end', dag=dag)
    
    # ==================== DEPENDENCIES - STEP BY STEP ====================
    
    # Layer 1: ETL - All 7 jobs run in parallel
    start >> [
        etl_tasks['etl_active_login'],
        etl_tasks['etl_active_logout'],
        etl_tasks['etl_recharge'],
        etl_tasks['etl_item_gain'],
        etl_tasks['etl_item_spend'],
        etl_tasks['etl_money_gain'],
        etl_tasks['etl_money_spend']
    ] >> etl_complete
    
    # Layer 2: STD - Single job to union all active sources
    etl_complete >> std_tasks['std_active'] >> std_complete
    
    # Layer 3a: CONS Date-dependent - 9 parallel jobs
    std_complete >> [
        cons_date_tasks['cons_active'],
        cons_date_tasks['cons_recharge'],
        cons_date_tasks['cons_cumulative_revenue'],
        cons_date_tasks['cons_item_gain'],
        cons_date_tasks['cons_item_spend'],
        cons_date_tasks['cons_money_gain'],
        cons_date_tasks['cons_money_spend'],
        cons_date_tasks['cons_server_performance'],
        cons_date_tasks['cons_farmers_performance']
    ] >> cons_date_complete
    
    # Layer 3b: CONS Snapshots - Run after date-dependent jobs
    # These are dimension tables that can be refreshed independently
    cons_date_complete >> [
        cons_snapshot_tasks['cons_guild_info'],
        cons_snapshot_tasks['cons_farmers_snapshot']
    ] >> end
    
    return dag


# ==================== DAG INSTANTIATION ====================
# Create DAG for L2M re-standardization

l2m_dag = create_l2m_dag('l2m')

# Register globally for Airflow discovery
globals()['l2m_re_standardization'] = l2m_dag
