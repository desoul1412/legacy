#!/usr/bin/env python3
"""
API Data Extractor - Universal API data extraction script

Supports multiple API sources:
- SensorTower: Market research data
- Facebook: Ads performance
- TikTok: Ads performance
- Google Ads: Ads performance

This script handles authentication, rate limiting, pagination, and error handling
for all supported APIs.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core.api_client import SensorTowerClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load API configuration file"""
    with open(config_path, 'r') as f:
        return json.load(f)


def load_credentials(api_source: str) -> dict:
    """Load API credentials from HDFS-downloaded files"""
    creds_dir = os.getenv('CREDENTIALS_DIR', '/tmp/gsbkk_creds')
    cred_file = f"{creds_dir}/cred_tsn.json"
    
    if not os.path.exists(cred_file):
        raise FileNotFoundError(f"Credentials file not found: {cred_file}")
    
    with open(cred_file, 'r') as f:
        all_creds = json.load(f)
    
    if api_source not in all_creds:
        raise KeyError(f"Credentials for {api_source} not found in {cred_file}")
    
    return all_creds[api_source]


def extract_sensortower(endpoint: str, config: dict, date: str, output_path: str, spark: SparkSession):
    """Extract data from SensorTower API"""
    logger.info(f"Extracting SensorTower data: {endpoint}")
    
    # Get credentials
    credentials = load_credentials('sensortower')
    api_key = credentials.get('api_key')
    
    # Initialize client
    client = SensorTowerClient(api_key=api_key)
    
    # Get endpoint-specific parameters from config
    endpoint_config = config.get('endpoints', {}).get(endpoint, {})
    countries = endpoint_config.get('countries', ['US'])
    categories = endpoint_config.get('categories', [])
    
    # Extract data based on endpoint
    all_data = []
    
    if endpoint == 'top_games':
        for country in countries:
            logger.info(f"Fetching top games for country: {country}")
            data = client.get_top_apps(
                country=country,
                category='game',
                device='iphone',
                date=date
            )
            # Add metadata
            for item in data:
                item['country'] = country
                item['extract_date'] = date
            all_data.extend(data)
    
    elif endpoint == 'new_games':
        for country in countries:
            logger.info(f"Fetching new games for country: {country}")
            data = client.get_new_apps(
                country=country,
                category='game',
                device='iphone',
                date=date
            )
            for item in data:
                item['country'] = country
                item['extract_date'] = date
            all_data.extend(data)
    
    elif endpoint == 'metadata':
        # Assuming we have app IDs to fetch metadata for
        app_ids = endpoint_config.get('app_ids', [])
        for app_id in app_ids:
            logger.info(f"Fetching metadata for app: {app_id}")
            data = client.get_app_details(app_id=app_id)
            data['extract_date'] = date
            all_data.append(data)
    
    else:
        raise ValueError(f"Unsupported SensorTower endpoint: {endpoint}")
    
    # Convert to Spark DataFrame and write to HDFS
    if all_data:
        logger.info(f"Extracted {len(all_data)} records")
        df = spark.createDataFrame(all_data)
        df.write.mode('overwrite').json(output_path)
        logger.info(f"Data written to: {output_path}")
    else:
        logger.warning("No data extracted")


def extract_facebook(endpoint: str, config: dict, date: str, output_path: str, spark: SparkSession):
    """Extract data from Facebook Ads API"""
    logger.info(f"Extracting Facebook data: {endpoint}")
    # TODO: Implement Facebook Ads extraction
    raise NotImplementedError("Facebook Ads extraction not yet implemented")


def extract_tiktok(endpoint: str, config: dict, date: str, output_path: str, spark: SparkSession):
    """Extract data from TikTok Ads API"""
    logger.info(f"Extracting TikTok data: {endpoint}")
    # TODO: Implement TikTok Ads extraction
    raise NotImplementedError("TikTok Ads extraction not yet implemented")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='API Data Extractor - Universal API data extraction'
    )
    parser.add_argument('--api-source', required=True, help='API source (sensortower, facebook, tiktok)')
    parser.add_argument('--endpoint', required=True, help='API endpoint to call')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--date', required=True, help='Extraction date (YYYY-MM or YYYY-MM-DD)')
    parser.add_argument('--output', required=True, help='Output path in HDFS')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("API Data Extraction")
    logger.info("=" * 60)
    logger.info(f"API Source: {args.api_source}")
    logger.info(f"Endpoint: {args.endpoint}")
    logger.info(f"Date: {args.date}")
    logger.info(f"Output: {args.output}")
    logger.info("=" * 60)
    
    # Load configuration
    config = load_config(args.config)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"API_Extraction_{args.api_source}_{args.endpoint}") \
        .getOrCreate()
    
    try:
        # Route to appropriate extraction function
        if args.api_source == 'sensortower':
            extract_sensortower(args.endpoint, config, args.date, args.output, spark)
        elif args.api_source == 'facebook':
            extract_facebook(args.endpoint, config, args.date, args.output, spark)
        elif args.api_source == 'tiktok':
            extract_tiktok(args.endpoint, config, args.date, args.output, spark)
        else:
            raise ValueError(f"Unsupported API source: {args.api_source}")
        
        logger.info("API extraction completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"API extraction failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
