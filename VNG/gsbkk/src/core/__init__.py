"""
Core components for GSBKK pipelines

This package contains the core data processing components:
- api_client: API clients for SensorTower, Facebook, TikTok
- writers: Output writers for HDFS, PostgreSQL, Google Sheets
- models: Dataclass models for configuration (LayoutConfig, InputSource, etc.)
- loaders: Data loading utilities (JDBC, file, GSheet)
- templates: SQL template rendering utilities
"""

# Always-available imports (no heavy dependencies)
from .models import LayoutConfig, InputSource, OutputConfig, JdbcCredentials

# Lazy imports for modules with dependencies (jinja2, pandas, pyspark, gspread)
def __getattr__(name):
    """Lazy import for modules with heavy dependencies"""
    # Templates (requires jinja2)
    if name == 'render_sql_template':
        from .templates import render_sql_template
        return render_sql_template
    elif name == 'resolve_table_path':
        from .templates import resolve_table_path
        return resolve_table_path
    elif name == 'substitute_variables':
        from .templates import substitute_variables
        return substitute_variables
    # API clients (requires pandas, requests)
    elif name == 'SensorTowerClient':
        from .api_client import SensorTowerClient
        return SensorTowerClient
    # Writers (requires pyspark, gspread)
    elif name == 'HadoopWriter':
        from .writers import HadoopWriter
        return HadoopWriter
    elif name == 'PostgresWriter':
        from .writers import PostgresWriter
        return PostgresWriter
    elif name == 'GSheetWriter':
        from .writers import GSheetWriter
        return GSheetWriter
    # Loaders (requires pyspark)
    elif name == 'get_jdbc_url':
        from .loaders import get_jdbc_url
        return get_jdbc_url
    elif name == 'load_file_source':
        from .loaders import load_file_source
        return load_file_source
    elif name == 'load_jdbc_source':
        from .loaders import load_jdbc_source
        return load_jdbc_source
    elif name == 'load_gsheet_source':
        from .loaders import load_gsheet_source
        return load_gsheet_source
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    # Models (always available)
    'LayoutConfig',
    'InputSource',
    'OutputConfig',
    'JdbcCredentials',
    # Templates (always available)
    'render_sql_template',
    'resolve_table_path',
    'substitute_variables',
    # API clients (lazy loaded)
    'SensorTowerClient',
    # Writers (lazy loaded)
    'HadoopWriter',
    'PostgresWriter',
    'GSheetWriter',
    # Loaders (lazy loaded)
    'get_jdbc_url',
    'load_file_source',
    'load_jdbc_source',
    'load_gsheet_source',
]

