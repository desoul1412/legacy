#!/usr/bin/env python3
################################################################################
# Pipeline Runner - Main Entry Point for Complex Workflows
################################################################################
#
# PURPOSE:
#   Main orchestrator for pipeline-based executions that require custom logic
#   beyond standard ETL transformations. Routes to specific pipeline
#   implementations based on pipeline type.
#
# ARCHITECTURE:
#   Config JSON → Load Config → Create Spark → Route to Pipeline → Execute
#
# SUPPORTED PIPELINES:
#   1. Market Research - SensorTower API data extraction
#   2. Game Health - Game diagnostic checks
#   3. Rolling Forecast - Forecast processing with Google Sheets
#   4. Re-standardization - Re-process historical data
#
# USAGE:
#   python3 run_pipeline.py market_research \
#     --config configs/pipelines/market_research.json \
#     --date 2024-12 \
#     --step extract_top_games
#
# CALLED BY:
#   run_pipeline.sh (when action is NOT 'etl', 'extract', or 'process')
#
# KEY RESPONSIBILITIES:
#   - Load and validate pipeline configuration
#   - Create Spark session with appropriate settings
#   - Route to specific pipeline implementation
#   - Handle environment variable substitution
#   - Pass runtime arguments (date, step, etc.)
#   - Manage Spark session lifecycle
#
# PIPELINE ROUTING:
#   - market_research → src/pipelines/market_research.py
#   - game_health → src/pipelines/game_health.py
#   - rolling_forecast → src/pipelines/rolling_forecast.py
#   - re_std → src/pipelines/re_standardization.py
#
# CONFIGURATION FORMAT:
#   {
#     "_context": {
#       "pipeline_name": "market_research_sensortower",
#       "description": "SensorTower market data extraction"
#     },
#     "api": {
#       "client_type": "sensortower",
#       "credentials_path": "hdfs://..."
#     },
#     "output": {
#       "type": "hdfs",
#       "path": "hdfs://c0s/data/raw/{logDate}",
#       "format": "json"
#     }
#   }
#
# AUTHOR: GSBKK Team
# VERSION: 2.3.0
# LAST UPDATED: December 2024
#
################################################################################

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.pipelines.market_research import MarketResearchPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """
    Load configuration from JSON file and substitute environment variables.
    
    Reads a JSON configuration file and replaces ${VAR_NAME} placeholders
    with values from environment variables. This allows configurations to
    reference secrets and dynamic values without hardcoding them.
    
    Args:
        config_path: Path to JSON configuration file
        
    Returns:
        Configuration dictionary with environment variables substituted
        
    Example:
        Config contains: "api_key": "${SENSORTOWER_API_KEY}"
        Environment has: SENSORTOWER_API_KEY=abc123
        Result: "api_key": "abc123"
    """
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Replace environment variables
    config_str = json.dumps(config)
    for key, value in os.environ.items():
        config_str = config_str.replace(f"${{{key}}}", value)
    
    return json.loads(config_str)


def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()


def run_market_research(config_path: str, date: str, step: str = 'all', countries: str = None):
    """
    Run market research pipeline
    
    Args:
        config_path: Path to config JSON file
        date: Date in YYYY-MM format
        step: Which step to run - 'all', 'extract_top_games', 'extract_new_games',
              'extract_metadata', 'extract_performance', 'process_etl', 
              'process_std', 'write_postgres'
        countries: Comma-separated country codes
    """
    logger.info(f"Starting Market Research Pipeline - Step: {step}")
    
    config = load_config(config_path)
    spark = create_spark_session(f"MarketResearch_{step}")
    
    pipeline = MarketResearchPipeline(spark, config)
    
    countries_list = countries.split(',') if countries else None
    # countries_list = ['VN']
    # pipeline.run(date, step, countries_list)
    pipeline.run(date, step)
    
    spark.stop()
    logger.info(f"Market Research Pipeline completed - Step: {step}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='GSBKK Data Pipeline Runner')
    parser.add_argument('pipeline', choices=['market_research'],
                       help='Pipeline to run')
    parser.add_argument('--config', required=True, help='Path to config file')
    parser.add_argument('--date', help='Date for pipeline (YYYY-MM or YYYY-MM-DD)')
    parser.add_argument('--step', default='all', 
                       help='Pipeline step (for market_research: all, extract_top_games, extract_new_games, extract_metadata, extract_performance)')
    parser.add_argument('--countries', help='Comma-separated country codes (for market_research)')
    
    args = parser.parse_args()
    
    try:
        if args.pipeline == 'market_research':
            run_market_research(args.config, args.date, args.step, args.countries)
        
        logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
