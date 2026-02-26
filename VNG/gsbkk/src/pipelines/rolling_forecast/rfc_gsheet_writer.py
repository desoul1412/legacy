#!/usr/bin/env python3
"""
Rolling Forecast Google Sheets Writer

Reads consolidated rolling forecast data from public.rfc_daily_{gameId} and
writes to appropriate Google Sheets based on game configuration.

Usage:
    python rfc_gsheet_writer.py <game_id> <log_date>
    
Examples:
    python rfc_gsheet_writer.py slth 2025-01-15
    python rfc_gsheet_writer.py pwmsea 2025-01-15
    python rfc_gsheet_writer.py cft 2025-01-15
"""

import sys
import json
import yaml
import os
from pathlib import Path

# Google Sheets API imports
from google.oauth2 import service_account
from googleapiclient.discovery import build

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, date_format

# Initialize Spark
spark = SparkSession.builder \
    .appName("RFC GSheet Writer") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# Constants
SERVICE_ACCOUNT_FILE = "/opt/airflow/dags/repo/tsn-data-0e06f020fc9b.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
RANGE_NAME = "Daily Actual!A3"
GAME_NAME_CONFIG = "/opt/airflow/dags/repo/configs/game_configs/game_name.yaml"
CREDENTIALS_DIR = "/opt/airflow/dags/repo/configs/credentials"


def load_postgres_credentials():
    """Load TSN Postgres credentials from configs/credentials/cred_tsn.json"""
    cred_file = os.path.join(CREDENTIALS_DIR, "cred_tsn.json")
    with open(cred_file, 'r') as f:
        creds = json.load(f)
    return creds.get('postgres', creds)


def get_game_config(game_id: str) -> dict:
    """Load game configuration from game_name.yaml"""
    with open(GAME_NAME_CONFIG, "r") as f:
        data = yaml.safe_load(f)
    
    games = data.get("games", {})
    if game_id not in games:
        raise ValueError(f"No configuration found for game_id: {game_id}")
    
    game_data = games[game_id]
    result = {}
    
    for entry in game_data:
        for market, details in entry.items():
            result[market] = {
                "game_name": details.get("game_name"),
                "country_code": details.get("country_code"),
                "rfc_sheet_id": details.get("rfc_sheet_id"),
            }
    
    return result


def get_consolidated_data(game_id: str, log_date: str = None) -> 'DataFrame':
    """Read consolidated data from public table
    
    Args:
        game_id: Game identifier
        log_date: Optional date filter (YYYY-MM-DD). If None, reads all data.
    """
    table_name = f"public.rfc_daily_{game_id}"
    
    # Load credentials
    pg_creds = load_postgres_credentials()
    jdbc_url = f"jdbc:postgresql://{pg_creds['host']}:{pg_creds['port']}/tsn_data"
    
    # Build query with optional date filter
    if log_date:
        query = f"(SELECT * FROM {table_name} WHERE report_date = '{log_date}') AS filtered_data"
    else:
        query = f"(SELECT * FROM {table_name}) AS all_data"
    
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", pg_creds["user"])
        .option("password", pg_creds["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    
    return df


def update_sheet(spreadsheet_id: str, data_df: 'DataFrame', log_date: str):
    """Update Google Sheet with incremental data for specific date
    
    Args:
        spreadsheet_id: Google Sheets ID
        data_df: DataFrame with data for specific date (should be 1 row)
        log_date: Date being updated (YYYY-MM-DD)
    """
    # Authenticate with service account
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=creds)
    
    # Convert to Pandas
    pdf = data_df.toPandas()
    
    if len(pdf) == 0:
        print(f"⚠ No data for {log_date}, skipping update")
        return
    
    # Handle data types
    for column in pdf.columns:
        if pdf[column].dtype == "object":
            pdf[column] = pdf[column].astype(str)
    
    pdf.fillna(0, inplace=True)
    
    # Convert to list of lists (should be 1 row for incremental update)
    new_rows = pdf.values.tolist()
    
    # Read existing data to find matching row
    existing_result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=RANGE_NAME
    ).execute()
    
    existing_values = existing_result.get('values', [])
    
    # Find row index for this date (first column is date)
    target_date = log_date  # Format: YYYY-MM-DD
    row_index = None
    
    for i, row in enumerate(existing_values):
        if len(row) > 0 and row[0] == target_date:
            row_index = i
            break
    
    if row_index is not None:
        # Update existing row
        # Calculate absolute row number (A3 is row 3, so add 3 + index)
        abs_row_num = 3 + row_index
        update_range = f"Daily Actual!A{abs_row_num}"
        
        body = {"values": new_rows}
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_range,
            valueInputOption="RAW",
            body=body,
        ).execute()
        
        print(f"✓ Updated row {abs_row_num} for date {log_date} ({result.get('updatedCells')} cells)")
    else:
        # Append new row
        # Insert at top (after header at A3)
        # First shift existing data down, then insert at A3
        append_range = "Daily Actual!A3"
        
        body = {"values": new_rows}
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=append_range,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
        
        print(f"✓ Inserted new row for date {log_date} ({result.get('updates', {}).get('updatedCells')} cells)")


def execute(game_id: str, log_date: str):
    """Main execution logic"""
    print(f"=== Rolling Forecast GSheet Writer for {game_id} on {log_date} ===")
    
    # Get game configuration
    game_config = get_game_config(game_id)
    print(f"Loaded configuration for {len(game_config)} markets")
    
    # Read consolidated data FOR SPECIFIC DATE ONLY
    print(f"Reading data from staging.rfc_daily_{game_id} WHERE report_date = '{log_date}'...")
    full_data = get_consolidated_data(game_id, log_date)
    
    # Get distinct countries
    countries = [row["country"] for row in full_data.select("country").distinct().collect()]
    print(f"Found countries: {countries}")
    
    # Determine update strategy based on game type
    if game_id in ["omgthai", "gnoth", "slth"]:
        # Single market games - only "*" market
        print(f"Single market game: updating main sheet only")
        sheet_id = game_config["*"]["rfc_sheet_id"]
        
        filtered_df = (
            full_data
            .withColumn("date", date_format("report_date", "yyyy-MM-dd"))
            .select("date", "install", "nru", "rr01", "dau", "pu", "npu", "rev_usd")
            .sort(desc("date"))
        )
        
        update_sheet(sheet_id, filtered_df, log_date)
        
    elif game_id in ["pwmsea", "l2m"]:
        # SEA aggregate + country breakdowns
        print(f"SEA market game: updating SEA sheet + individual countries")
        
        # Update SEA aggregate
        sheet_id = game_config["*"]["rfc_sheet_id"]
        sea_df = (
            full_data
            .filter(col("country") == "SEA")
            .withColumn("date", date_format("report_date", "yyyy-MM-dd"))
            .select("date", "install", "nru", "rr01", "dau", "pu", "npu", "rev_usd")
            .sort(desc("date"))
        )
        update_sheet(sheet_id, sea_df, log_date)
        
        # Update individual countries
        for country in countries:
            if country != "SEA" and country is not None and country != "na":
                if country in game_config:
                    sheet_id = game_config[country]["rfc_sheet_id"]
                    country_df = (
                        full_data
                        .filter(col("country") == country)
                        .withColumn("date", date_format("report_date", "yyyy-MM-dd"))
                        .select("date", "install", "nru", "rr01", "dau", "pu", "npu", "rev_usd")
                        .sort(desc("date"))
                    )
                    update_sheet(sheet_id, country_df, log_date)
                else:
                    print(f"⚠ Warning: No sheet configuration for country {country}, skipping")
    
    else:
        # Multi-country games - each country has separate sheet
        print(f"Multi-country game: updating individual country sheets")
        for country in countries:
            if country in game_config:
                sheet_id = game_config[country]["rfc_sheet_id"]
                country_df = (
                    full_data
                    .filter(col("country") == country)
                    .withColumn("date", date_format("report_date", "yyyy-MM-dd"))
                    .select("date", "install", "nru", "rr01", "dau", "pu", "npu", "rev_usd")
                    .sort(desc("date"))
                )
                update_sheet(sheet_id, country_df, log_date)
            else:
                print(f"⚠ Warning: No sheet configuration for country {country}, skipping")
    
    print(f"✓ Completed Google Sheets update for {game_id}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python rfc_gsheet_writer.py <game_id> <log_date>")
        print("Example: python rfc_gsheet_writer.py slth 2025-01-15")
        sys.exit(1)
    
    game_id = sys.argv[1]
    log_date = sys.argv[2]
    
    execute(game_id, log_date)
