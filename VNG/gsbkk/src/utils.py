################################################################################
# Utility Functions - Common Helpers for GSBKK Pipelines
################################################################################
#
# PURPOSE:
#   Shared utility functions used across all GSBKK pipelines.
#   Provides common functionality to avoid code duplication.
#
# KEY FUNCTIONS:
#   - Environment management (load_env_file)
#   - Date handling (parse_date_range, get_yesterday, get_last_month)
#   - Path formatting (format_path)
#   - Configuration validation (validate_config)
#   - List operations (chunk_list)
#   - Retry logic (retry_on_failure)
#   - Notifications (send_notification)
#
# USAGE EXAMPLES:
#   from src.utils import parse_date_range, format_path
#   
#   # Generate date range
#   dates = parse_date_range('2024-12-01', '2024-12-31')
#   
#   # Format HDFS path
#   path = format_path(
#       'hdfs://c0s/data/{game_id}/{date}',
#       game_id='l2m',
#       date='2024-12-25'
#   )
#   
#   # Retry API call
#   def fetch_data():
#       return api_client.get_data()
#   data = retry_on_failure(fetch_data, max_retries=3, delay=5)
#
# USED BY:
#   - All pipeline implementations in src/pipelines/
#   - ETL engine for date substitution
#   - DAG factory for notification handling
#
# AUTHOR: GSBKK Team
# VERSION: 2.3.0
# LAST UPDATED: December 2024
#
################################################################################

import os
import json
import logging
from typing import Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def load_env_file(env_file: str = '.env'):
    """
    Load environment variables from .env file.
    Useful for local development when HDFS credentials are not available.
    
    Format:
        KEY=value
        ANOTHER_KEY=another_value
    
    Args:
        env_file: Path to .env file
    """
    if not os.path.exists(env_file):
        logger.warning(f"Environment file not found: {env_file}")
        return
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()
    
    logger.info(f"Loaded environment from {env_file}")


def parse_date_range(start_date: str, end_date: str) -> list:
    """
    Generate list of dates between start and end
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        List of date strings
    """
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates


def format_path(path_template: str, **kwargs) -> str:
    """
    Format path template with variables
    
    Args:
        path_template: Template string with {var} placeholders
        **kwargs: Variables to substitute
        
    Returns:
        Formatted path
    """
    return path_template.format(**kwargs)


def validate_config(config: Dict[str, Any], required_keys: list) -> bool:
    """
    Validate configuration has required keys
    
    Args:
        config: Configuration dictionary
        required_keys: List of required keys
        
    Returns:
        True if valid, raises ValueError otherwise
    """
    missing = [key for key in required_keys if key not in config]
    
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")
    
    return True


def get_yesterday() -> str:
    """Get yesterday's date in YYYY-MM-DD format"""
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


def get_last_month() -> str:
    """Get last month in YYYY-MM format"""
    today = datetime.now()
    first_this_month = today.replace(day=1)
    last_month = first_this_month - timedelta(days=1)
    return last_month.strftime('%Y-%m')


def chunk_list(lst: list, chunk_size: int) -> list:
    """
    Split list into chunks
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def retry_on_failure(func, max_retries: int = 3, delay: int = 5):
    """
    Retry function on failure
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        delay: Delay between retries in seconds
        
    Returns:
        Function result
    """
    import time
    
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise


def send_notification(message: str, webhook_url: str = None):
    """
    Send notification to webhook (e.g., Slack, Teams)
    
    Args:
        message: Notification message
        webhook_url: Webhook URL (from env if not provided)
    """
    import requests
    
    if not webhook_url:
        webhook_url = os.getenv('NOTIFICATION_WEBHOOK_URL')
    
    if not webhook_url:
        logger.warning("No webhook URL configured for notifications")
        return
    
    try:
        payload = {'text': message}
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info("Notification sent successfully")
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")
