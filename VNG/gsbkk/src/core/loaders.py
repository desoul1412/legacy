"""
Data loaders for ETL Engine

Handles reading data from various sources (file, JDBC, GSheet).
"""

import os
import json
import logging
from typing import Dict, Any, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


def get_jdbc_url(connection: str, variables: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    """
    Resolve JDBC connection with variable substitution

    Args:
        connection: Connection string (may contain {gameId}, {database}, etc.)
        variables: Variables for substitution

    Returns:
        (jdbc_url, properties_dict)
    """
    creds_dir = os.getenv('CREDENTIALS_DIR', '/opt/airflow/dags/repo/configs')

    def load_creds(filename: str) -> Dict[str, Any]:
        cred_file = os.path.join(creds_dir, filename)
        if os.path.exists(cred_file):
            with open(cred_file, 'r') as f:
                return json.load(f)
        return {}

    tsn_creds = load_creds('cred_tsn.json')
    gds_creds = load_creds('cred_gds.json')
    trino_creds = load_creds('cred_trino.json')

    # Substitute variables in connection string
    for key, value in variables.items():
        connection = connection.replace(f"{{{key}}}", str(value))

    # Handle connection aliases
    if connection == 'TSN_POSTGRES':
        pg_creds = tsn_creds.get('postgres', tsn_creds)
        database = variables.get('gameId')
        if not database or database == 'None':
            database = 'sensortower'
        jdbc_url = f"jdbc:postgresql://{pg_creds['host']}:{pg_creds['port']}/{database}"
        properties = {
            'user': pg_creds['user'],
            'password': pg_creds['password'],
            'driver': 'org.postgresql.Driver'
        }
        return jdbc_url, properties

    elif connection == 'GDS_POSTGRES':
        pg_creds = gds_creds.get('postgres', gds_creds)
        database = variables.get('gameId', variables.get('database'))
        if not database or str(database) == 'None':
            database = 'sensortower'
        jdbc_url = f"jdbc:postgresql://{pg_creds['host']}:{pg_creds['port']}/{database}"
        properties = {
            'user': pg_creds['user'],
            'password': pg_creds['password'],
            'driver': 'org.postgresql.Driver'
        }
        return jdbc_url, properties

    elif connection == 'GDS_TRINO':
        trino_cfg = trino_creds.get('trino', trino_creds)
        jdbc_url = f"jdbc:trino://{trino_cfg['host']}:{trino_cfg['port']}/iceberg/default?SSL=true&SSLVerification=NONE"
        properties = {
            'user': trino_cfg['user'],
            'password': trino_creds['password'],
            'driver': 'io.trino.jdbc.TrinoDriver'
        }
        return jdbc_url, properties

    elif connection.startswith('jdbc:'):
        return connection, {}

    else:
        raise ValueError(f"Unknown connection: {connection}")


def load_file_source(spark: SparkSession, path: str, format: str = 'parquet') -> DataFrame:
    """Load data from file source (HDFS/local)"""
    logger.info(f"Loading file source: {path} (format: {format})")
    return spark.read.format(format).load(path)


def load_jdbc_source(
    spark: SparkSession,
    connection: str,
    variables: Dict[str, Any],
    table: Optional[str] = None,
    query: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None
) -> DataFrame:
    """
    Load data from JDBC source
    
    Args:
        spark: SparkSession
        connection: Connection alias (TSN_POSTGRES, GDS_POSTGRES, etc.)
        variables: Variables for substitution
        table: Table name or subquery
        query: SQL query (alternative to table)
        properties: Additional JDBC properties
    """
    jdbc_url, conn_properties = get_jdbc_url(connection, variables)
    
    if properties:
        conn_properties.update(properties)
    
    # Use query or table
    dbtable = table if table else f"({query}) AS subquery"
    
    logger.info(f"Loading JDBC source: {jdbc_url} / {dbtable[:100]}...")
    
    return spark.read.jdbc(url=jdbc_url, table=dbtable, properties=conn_properties)


def load_gsheet_source(
    spark: SparkSession,
    sheet_id: str,
    worksheet_name: str = 'Sheet1',
    creds_path: Optional[str] = None
) -> DataFrame:
    """
    Load data from Google Sheets
    
    Args:
        spark: SparkSession
        sheet_id: Google Sheet ID
        worksheet_name: Name of worksheet to read
        creds_path: Path to service account credentials
    """
    from src.core.writers import GSheetWriter
    
    if creds_path is None:
        creds_dir = os.getenv('CREDENTIALS_DIR', '/tmp/gsbkk_creds')
        creds_path = f"{creds_dir}/gsheet_creds.json"
    
    if not os.path.exists(creds_path):
        raise ValueError(f"Google Sheets credentials not found at {creds_path}")
    
    logger.info(f"Loading GSheet source: {sheet_id}/{worksheet_name}")
    
    gsheet_reader = GSheetWriter(creds_path)
    return gsheet_reader.read(sheet_id, worksheet_name)
