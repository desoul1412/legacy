"""
L2M Top VIP Analysis DAG

This DAG performs re-standardization for L2M to analyze top VIP users.
It processes raw game logs through ETL, STD, and CONS stages to create
cumulative revenue and VIP-focused consolidated metrics.

Pipeline Flow:
  ETL Stage: Raw logs → Standardized parquet files (recharge, active, revenue)
  STD Stage: Parquet → PostgreSQL standardized tables
  CONS Stage: Multi-source joins → Consolidated VIP analysis tables

Focus: Top VIP users and cumulative revenue analysis for L2M

Author: GSBKK Team
Date: 2025-12-26
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


def create_re_std_dag(game_id='l2m'):
    """
    Create an L2M Top VIP analysis DAG using declarative pipeline config
    
    Args:
        game_id: Game identifier (default: 'l2m')
    """
    
    dag = DAG(
        f'l2m_top_vip_analysis',
        default_args=default_args,
        description=f'L2M Top VIP user analysis and cumulative revenue tracking',
        schedule_interval='0 5 * * *',  # Run at 5 AM daily
        catchup=False,
        tags=['l2m', 'vip_analysis', 'top_users', 'etl'],
    )
    
    # Define pipeline configuration - step by step for easier debugging
    pipeline = [
        # Step 1: Extract recharge data
        {'name': 'etl_recharge', 'type': 'etl',
         'layout': f'layouts/re-standardization/etl/{game_id}_recharge.json'},
        
        # Step 2: Extract active user data
        {'name': 'etl_active', 'type': 'etl',
         'layout': f'layouts/re-standardization/etl/{game_id}_active.json'},
        
        # Step 3: Extract daily revenue data
        {'name': 'etl_daily_revenue', 'type': 'etl',
         'layout': f'layouts/re-standardization/etl/{game_id}_daily_revenue.json'},
        
        # Step 4: Standardize recharge data to PostgreSQL
        {'name': 'std_recharge', 'type': 'etl',
         'layout': f'layouts/re-standardization/std/{game_id}_recharge.json'},
        
        # Step 5: Standardize active user data to PostgreSQL
        {'name': 'std_active', 'type': 'etl',
         'layout': f'layouts/re-standardization/std/{game_id}_active.json'},
        
        # Step 6: Consolidate cumulative revenue (VIP analysis)
        {'name': 'cons_cumulative_revenue', 'type': 'etl',
         'layout': f'layouts/re-standardization/cons/{game_id}_cumulative_revenue.json'},
    ]
    
    # Create all tasks from configuration
    tasks = create_pipeline_tasks(dag, pipeline, game_id=game_id)
    
    # Control flow tasks
    start = EmptyOperator(task_id='start', dag=dag)
    etl_complete = EmptyOperator(task_id='etl_complete', dag=dag)
    std_complete = EmptyOperator(task_id='std_complete', dag=dag)
    end = EmptyOperator(task_id='end', dag=dag)
    
    # ==================== DEPENDENCIES - STEP BY STEP ====================
    # Step 1: ETL phase (parallel extraction of all raw data)
    start >> [tasks['etl_recharge'], tasks['etl_active'], 
              tasks['etl_daily_revenue']] >> etl_complete
    
    # Step 2: STD phase (parallel standardization of recharge and active)
    etl_complete >> [tasks['std_recharge'], tasks['std_active']] >> std_complete
    
    # Step 3: CONS phase (cumulative revenue depends on STD + daily_revenue)
    [std_complete, tasks['etl_daily_revenue']] >> tasks['cons_cumulative_revenue'] >> end
    
    return dag


# ==================== DAG INSTANTIATION ====================
# Create DAG for L2M top VIP analysis

# Create the DAG
l2m_dag = create_re_std_dag('l2m')

# Register globally for Airflow discovery
globals()['l2m_top_vip_analysis'] = l2m_dag
