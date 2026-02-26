"""
SQL template rendering utilities

Handles Jinja2 template rendering for SQL queries.
"""

import os
import logging
from typing import Dict, Any, Union, List

logger = logging.getLogger(__name__)


def render_sql_template(sql_template_path: str, variables: Dict[str, Any]) -> str:
    """
    Render a Jinja2 SQL template with given variables.

    Args:
        sql_template_path: Path to SQL template file (relative or absolute)
        variables: Dictionary of variables to pass to template

    Returns:
        Rendered SQL query string
    """
    # Lazy import jinja2 to avoid import errors in environments without it
    from jinja2 import Environment, FileSystemLoader
    
    # Set up Jinja2 environment with multiple search paths for imports
    template_dir = os.path.dirname(sql_template_path) if not os.path.isabs(
        sql_template_path) else os.path.dirname(sql_template_path)
    template_name = os.path.basename(sql_template_path)

    # If relative path, resolve from project root
    if not os.path.isabs(template_dir):
        project_root = os.getenv('PROJECT_ROOT', '/opt/airflow/dags/repo')
        template_dir = os.path.join(project_root, template_dir)

    # Add both the template directory and common templates/sql for imports/macros
    project_root = os.getenv('PROJECT_ROOT', '/opt/airflow/dags/repo')
    search_paths = [
        template_dir,                                      # Template's own directory
        os.path.join(project_root, 'templates/sql'),      # Common SQL macros
        os.path.join(project_root, 'templates'),          # Templates root
    ]

    env = Environment(loader=FileSystemLoader(search_paths))
    template = env.get_template(template_name)

    # Add snake_case aliases for Jinja2 templates (which use snake_case convention)
    template_vars = dict(variables)
    if 'logDate' in template_vars:
        template_vars['log_date'] = template_vars['logDate']
    if 'gameId' in template_vars:
        template_vars['game_id'] = template_vars['gameId']

    # Render SQL with variables
    return template.render(**template_vars)


def resolve_table_path(game_id: str, table_key: str) -> str:
    """
    Resolve table path from data_path.yaml based on game_id and table_key

    Args:
        game_id: Game identifier (e.g., 'cft', 'mlb', 'slth')
        table_key: Table key from layout (e.g., 'active', 'charge', 'user_profile')

    Returns:
        Full table path (e.g., 'public.mkt_user_active' or 'mkt.daily_user_active')
    """
    import yaml

    config_path = os.getenv(
        'DATA_PATH_CONFIG', '/opt/airflow/dags/repo/configs/data_path.yaml')

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        warehouse = config.get('warehouse', {})
        game_tables = warehouse.get(game_id, {})

        if table_key not in game_tables:
            raise ValueError(
                f"Table key '{table_key}' not found for game '{game_id}' in data_path.yaml")

        return game_tables[table_key]

    except FileNotFoundError:
        logger.warning(f"data_path.yaml not found at {config_path}, skipping table resolution")
        return f"{{{table_key}Table}}"  # Return placeholder as fallback
    except Exception as e:
        logger.warning(f"Error resolving table path: {e}")
        return f"{{{table_key}Table}}"


def substitute_variables(obj: Any, variables: Dict[str, Any]) -> Any:
    """
    Recursively replace {variable} placeholders in strings, dicts, and lists.
    
    Args:
        obj: Object to process (str, dict, list, or other)
        variables: Variables to substitute
        
    Returns:
        Object with variables substituted
    """
    if isinstance(obj, dict):
        return {k: substitute_variables(v, variables) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [substitute_variables(item, variables) for item in obj]
    elif isinstance(obj, str):
        result = obj
        for key, value in variables.items():
            result = result.replace(f"{{{key}}}", str(value))
        return result
    else:
        return obj
