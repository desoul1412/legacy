"""
Data Writers for Hadoop (HDFS) and PostgreSQL
Supports append and overwrite modes
"""

import logging
from typing import Dict, List, Optional, Any, Literal
from pyspark.sql import DataFrame
import subprocess
import gspread

logger = logging.getLogger(__name__)


class Writer:
    """Base writer class"""
    
    def write(self, df: DataFrame, **kwargs):
        """Write DataFrame to destination"""
        raise NotImplementedError


class HadoopWriter(Writer):
    """Write data to Hadoop HDFS"""
    
    def __init__(self, base_path: str = "hdfs://c0s/user/gsbkk-workspace-yc9t6"):
        """
        Initialize Hadoop writer
        
        Args:
            base_path: Base HDFS path
        """
        self.base_path = base_path.rstrip('/')
    
    def write(self, df: DataFrame, path: str, mode: Literal['append', 'overwrite'] = 'append',
             format: str = 'parquet', partition_by: Optional[List[str]] = None,
             num_partitions: Optional[int] = None, **options):
        """
        Write DataFrame to HDFS
        
        Args:
            df: DataFrame to write
            path: Relative path from base_path
            mode: Write mode ('append' or 'overwrite')
            format: Output format (default: 'parquet')
            partition_by: Columns to partition by
            num_partitions: Number of partitions (for repartition)
            **options: Additional write options
        """
        full_path = f"{self.base_path}/{path.lstrip('/')}"
        
        logger.info(f"Writing to HDFS: {full_path} (mode={mode}, format={format})")
        
        # Repartition if specified
        if num_partitions:
            df = df.repartition(num_partitions)
        
        writer = df.write.mode(mode).format(format)
        
        # Add partition columns
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        # Apply options
        if options:
            writer = writer.options(**options)
        
        writer.save(full_path)
        logger.info(f"Successfully wrote {df.count()} records to {full_path}")
    
    def delete_path(self, path: str):
        """
        Delete HDFS path
        
        Args:
            path: Relative path from base_path
        """
        full_path = f"{self.base_path}/{path.lstrip('/')}"
        logger.info(f"Deleting HDFS path: {full_path}")
        
        try:
            subprocess.run(
                ['hdfs', 'dfs', '-rm', '-r', '-f', full_path],
                check=True,
                capture_output=True
            )
            logger.info(f"Successfully deleted {full_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to delete {full_path}: {e}")


class PostgresWriter(Writer):
    """Write data to PostgreSQL"""
    
    def __init__(self, host: str, port: int, user: str, password: str,
                 driver: str = "org.postgresql.Driver"):
        """
        Initialize Postgres writer
        
        Args:
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            driver: JDBC driver class
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.driver = driver
    
    def write(self, df: DataFrame, database: str, schema: str, table: str,
             mode: Literal['append', 'overwrite'] = 'append',
             batch_size: int = 10000, **options):
        """
        Write DataFrame to PostgreSQL
        
        Args:
            df: DataFrame to write
            database: Database name
            schema: Schema name
            table: Table name
            mode: Write mode ('append' or 'overwrite')
            batch_size: Batch size for inserts
            **options: Additional JDBC options
        """
        jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{database}"
        full_table = f"{schema}.{table}"
        
        logger.info(f"Writing to PostgreSQL: {full_table} (mode={mode})")
        
        write_options = {
            'url': jdbc_url,
            'dbtable': full_table,
            'user': self.user,
            'password': self.password,
            'driver': self.driver,
            'batchsize': batch_size,
            **options
        }
        
        df.write.mode(mode).format('jdbc').options(**write_options).save()
        logger.info(f"Successfully wrote {df.count()} records to {full_table}")
    
    def truncate_table(self, database: str, schema: str, table: str):
        """
        Truncate table before writing
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
        """
        import psycopg2
        
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=database,
            user=self.user,
            password=self.password
        )
        
        try:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {schema}.{table}")
                conn.commit()
                logger.info(f"Truncated table {schema}.{table}")
        finally:
            conn.close()
    
    def create_table_if_not_exists(self, df: DataFrame, database: str, 
                                   schema: str, table: str, keys: Optional[str] = None):
        """
        Create table if it doesn't exist
        
        Args:
            df: DataFrame to infer schema from
            database: Database name
            schema: Schema name
            table: Table name
            keys: Primary key columns (comma-separated)
        """
        import psycopg2
        
        # Map Spark types to Postgres types
        type_mapping = {
            'StringType': 'TEXT',
            'IntegerType': 'INTEGER',
            'LongType': 'BIGINT',
            'DoubleType': 'DOUBLE PRECISION',
            'FloatType': 'REAL',
            'BooleanType': 'BOOLEAN',
            'DateType': 'DATE',
            'TimestampType': 'TIMESTAMP',
            'DecimalType': 'DECIMAL',
        }
        
        # Generate CREATE TABLE statement
        columns = []
        for field in df.schema.fields:
            pg_type = type_mapping.get(type(field.dataType).__name__, 'TEXT')
            nullable = "NULL" if field.nullable else "NOT NULL"
            columns.append(f'"{field.name}" {pg_type} {nullable}')
        
        if keys:
            columns.append(f"PRIMARY KEY ({keys})")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            {', '.join(columns)}
        )
        """
        
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=database,
            user=self.user,
            password=self.password
        )
        
        try:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                conn.commit()
                logger.info(f"Created table {schema}.{table} if not exists")
        finally:
            conn.close()


class GSheetWriter:
    """Write data to Google Sheets"""
    
    def __init__(self, credentials_path: str):
        """
        Initialize Google Sheets writer
        
        Args:
            credentials_path: Path to service account JSON
        """
        import gspread
        from google.oauth2.service_account import Credentials
        
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        self.client = gspread.authorize(creds)
    
    def write(self, df: DataFrame, sheet_id: str, worksheet_name: str,
             mode: Literal['append', 'overwrite'] = 'overwrite',
             start_cell: str = 'A1'):
        """
        Write DataFrame to Google Sheet
        
        Args:
            df: DataFrame to write
            sheet_id: Google Sheet ID
            worksheet_name: Worksheet name
            mode: Write mode ('append' or 'overwrite')
            start_cell: Starting cell (default: 'A1')
        """
        import pandas as pd
        
        logger.info(f"Writing to Google Sheet: {sheet_id}/{worksheet_name} (mode={mode})")
        
        # Convert to pandas
        pdf = df.toPandas()
        
        # Open worksheet
        sheet = self.client.open_by_key(sheet_id)
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
        
        # Clear if overwrite
        if mode == 'overwrite':
            worksheet.clear()
        
        # Write data
        worksheet.update([pdf.columns.values.tolist()] + pdf.values.tolist(), start_cell)
        logger.info(f"Successfully wrote {len(pdf)} records to {worksheet_name}")
    
    def read(self, sheet_id: str, worksheet_name: str) -> DataFrame:
        """
        Read data from Google Sheet
        
        Args:
            sheet_id: Google Sheet ID
            worksheet_name: Worksheet name
            
        Returns:
            DataFrame
        """
        import pandas as pd
        from pyspark.sql import SparkSession
        
        logger.info(f"Reading from Google Sheet: {sheet_id}/{worksheet_name}")
        
        sheet = self.client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        
        # Get all values
        data = worksheet.get_all_values()
        
        # Convert to DataFrame
        if len(data) > 1:
            pdf = pd.DataFrame(data[1:], columns=data[0])
            spark = SparkSession.builder.getOrCreate()
            return spark.createDataFrame(pdf)
        else:
            return None


def create_writer(writer_type: str, **kwargs) -> Writer:
    """
    Factory function to create writers
    
    Args:
        writer_type: Type of writer ('hadoop', 'postgres', 'gsheet')
        **kwargs: Writer configuration
        
    Returns:
        Writer instance
    """
    writers = {
        'hadoop': HadoopWriter,
        'postgres': PostgresWriter,
        'gsheet': GSheetWriter,
    }
    
    if writer_type not in writers:
        raise ValueError(f"Unknown writer type: {writer_type}")
    
    return writers[writer_type](**kwargs)
