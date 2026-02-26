#!/usr/bin/env python3
"""
Google Sheets Data Processor

Handles reading from and writing to Google Sheets.
Used for rolling forecast and manual data entry workflows.

Actions:
- read: Read data from Google Sheets and write to Postgres/HDFS
- write: Read data from HDFS and write to Google Sheets
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core.writers import GSheetWriter, PostgresWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load Google Sheets configuration"""
    with open(config_path, 'r') as f:
        return json.load(f)


def load_credentials() -> dict:
    """Load Postgres credentials"""
    creds_dir = os.getenv('CREDENTIALS_DIR', '/tmp/gsbkk_creds')
    cred_file = f"{creds_dir}/cred_tsn.json"
    
    with open(cred_file, 'r') as f:
        return json.load(f)


def read_from_gsheet(config: dict, date: str, spark: SparkSession):
    """
    Read data from Google Sheets and write to Postgres
    
    Used for rolling forecast: Read daily/monthly forecasts from sheets
    """
    logger.info("Reading data from Google Sheets")
    
    # Get Google Sheets configuration
    sheet_id = config['sheet_id']
    range_name = config['range']
    table_name = config['output_table']
    
    # Read from Google Sheets
    gsheet_writer = GSheetWriter()
    data = gsheet_writer.read_from_sheet(sheet_id, range_name)
    
    if not data:
        logger.warning("No data read from Google Sheets")
        return
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(data)
    
    # Get Postgres configuration
    postgres_config = load_credentials().get('postgres', {})
    
    # Write to Postgres
    logger.info(f"Writing {df.count()} rows to Postgres table: {table_name}")
    
    postgres_writer = PostgresWriter(
        host=postgres_config['host'],
        port=postgres_config['port'],
        database=postgres_config['database'],
        user=postgres_config['user'],
        password=postgres_config['password']
    )
    
    write_mode = config.get('write_mode', 'overwrite')
    postgres_writer.write(df, table_name, mode=write_mode)
    
    logger.info("Data successfully written to Postgres")


def write_to_gsheet(spark: SparkSession, config: dict, date: str = None, df_input=None):
    """
    Read data from HDFS/Postgres/DataFrame and write to Google Sheets
    
    Used for rolling forecast: Write diagnostic summaries or RFC data to sheets
    
    Args:
        spark: SparkSession
        config: Configuration dict with sheet_id, range, input_type, etc.
        date: Date string for filtering (optional if df_input provided)
        df_input: Pre-loaded DataFrame (optional, bypasses input reading)
    """
    logger.info("Writing data to Google Sheets")
    
    # Get input configuration
    input_type = config.get('input_type', 'hdfs')  # 'hdfs', 'postgres', or 'dataframe'
    sheet_id = config['sheet_id']
    range_name = config['range']
    
    # Use provided DataFrame or read from source
    if df_input is not None:
        logger.info("Using provided DataFrame")
        df = df_input
    elif input_type == 'postgres':
        # Read from Postgres
        table_name = config['input_table'].replace('{date}', date)
        logger.info(f"Reading data from Postgres table: {table_name}")
        
        # Load credentials
        postgres_config = load_credentials().get('postgres', {})
        jdbc_url = f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config.get('database', 'tsn_data')}"
        
        # Build query with optional date filter
        if date:
            date_column = config.get('date_column', 'report_date')
            query = f"(SELECT * FROM {table_name} WHERE {date_column} = '{date}') AS filtered_data"
        else:
            query = f"(SELECT * FROM {table_name}) AS data"
        
        df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("user", postgres_config["user"])
            .option("password", postgres_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    else:
        # Read from HDFS (original behavior)
        if not date:
            raise ValueError("date parameter required for HDFS input")
        input_path = config['input_path'].replace('{date}', date)
        logger.info(f"Reading data from HDFS: {input_path}")
        df = spark.read.parquet(input_path)
    
    # Apply column selection if specified
    if 'columns' in config:
        df = df.select(*config['columns'])
    
    # Apply sorting if specified
    if 'sort_by' in config:
        from pyspark.sql.functions import desc, asc
        sort_cols = []
        for col_spec in config['sort_by']:
            if isinstance(col_spec, dict):
                col_name = col_spec['column']
                order = col_spec.get('order', 'asc')
                sort_cols.append(desc(col_name) if order == 'desc' else asc(col_name))
            else:
                sort_cols.append(col_spec)
        df = df.sort(*sort_cols)
    
    # Convert to list of lists for Google Sheets API
    rows = df.collect()
    if not rows:
        logger.warning("No data to write")
        return
    
    # Convert to values (list of lists) with proper number formatting
    data = []
    for row in rows:
        row_data = []
        for col in df.columns:
            val = row[col]
            if val is None:
                row_data.append('')
            elif isinstance(val, (int, float)):
                # Round floats to 2 decimal places
                if isinstance(val, float):
                    row_data.append(round(val, 2))
                else:
                    row_data.append(val)
            else:
                row_data.append(str(val))
        data.append(row_data)
    
    logger.info(f"Writing {len(data)} rows to Google Sheets")
    
    # Write to Google Sheets using service account
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    
    service_account_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not service_account_file:
        service_account_file = f"{os.getenv('CREDENTIALS_DIR', '/tmp/gsbkk_creds')}/tsn-data-0e06f020fc9b.json"
    
    creds = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    
    service = build('sheets', 'v4', credentials=creds)
    
    # Determine update mode
    update_mode = config.get('update_mode', 'append')  # 'append', 'overwrite', or 'upsert'
    
    if update_mode == 'overwrite':
        # Clear and write
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        # Write header + data
        values = [df.columns] + data
        body = {'values': values}
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        logger.info(f"Overwrote sheet with {len(data)} rows")
        
    elif update_mode == 'upsert':
        # Update existing rows or insert new based on key_column
        key_column = config.get('key_column', df.columns[0])  # Default to first column
        
        # Find the index of the key column
        try:
            key_index = df.columns.index(key_column)
        except ValueError:
            raise ValueError(f"key_column '{key_column}' not found in DataFrame columns: {df.columns}")
        
        # Get existing data
        existing = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        existing_values = existing.get('values', [])
        range_start_row = int(range_name.split('!')[1].replace('A', ''))
        sheet_name = range_name.split('!')[0]
        
        # Process each row in data
        updated_count = 0
        inserted_count = 0
        
        # Helper function to normalize dates for comparison
        def normalize_date(value):
            """Convert various date formats to YYYY-MM-DD string"""
            if value is None or value == '':
                return None
            
            value_str = str(value).strip()
            
            # Try parsing common date formats
            from datetime import datetime
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(value_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # If it's already in YYYY-MM-DD format, return as-is
            if len(value_str) == 10 and value_str[4] == '-' and value_str[7] == '-':
                return value_str
            
            # Return original if can't parse
            return value_str
        
        for data_row in data:
            search_key = data_row[key_index] if len(data_row) > key_index else None
            # Normalize search_key to date format
            search_key_normalized = normalize_date(search_key)
            
            # Find matching row in existing data
            row_index = None
            for i, existing_row in enumerate(existing_values):
                if len(existing_row) > key_index:
                    existing_key_normalized = normalize_date(existing_row[key_index])
                    if existing_key_normalized == search_key_normalized:
                        row_index = i
                        logger.info(f"Found match: '{existing_key_normalized}' == '{search_key_normalized}' at row {i}")
                        break
            
            if row_index is None:
                logger.info(f"No match found for '{search_key_normalized}'. Will insert new row.")
            
            if row_index is not None:
                # Update existing row
                abs_row = range_start_row + row_index
                update_range = f"{sheet_name}!A{abs_row}"
                
                body = {'values': [data_row]}
                service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=update_range,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                updated_count += 1
                logger.info(f"Updated row {abs_row} (key='{search_key}')")
            else:
                # Append new row
                body = {'values': [data_row]}
                service.spreadsheets().values().append(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                    body=body
                ).execute()
                inserted_count += 1
                logger.info(f"Inserted new row (key='{search_key}')")
        
        logger.info(f"Upsert complete: {updated_count} updated, {inserted_count} inserted")
    else:
        # Append mode (original behavior)
        body = {'values': data}
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        logger.info(f"Appended {len(data)} rows")
    
    logger.info("Data successfully written to Google Sheets")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Google Sheets Data Processor'
    )
    parser.add_argument('--action', required=True, choices=['read', 'write'],
                       help='Action to perform: read from or write to Google Sheets')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--date', required=True, help='Processing date')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Google Sheets Processing")
    logger.info("=" * 60)
    logger.info(f"Action: {args.action}")
    logger.info(f"Date: {args.date}")
    logger.info("=" * 60)
    
    # Load configuration
    config = load_config(args.config)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"GSheet_{args.action.capitalize()}_{args.date}") \
        .getOrCreate()
    
    try:
        if args.action == 'read':
            read_from_gsheet(config, args.date, spark)
        else:  # write
            write_to_gsheet(config, args.date, spark)
        
        logger.info(f"Google Sheets {args.action} completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Google Sheets processing failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
