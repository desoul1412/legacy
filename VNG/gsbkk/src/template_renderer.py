#!/usr/bin/env python3
"""
Jinja2 Template Renderer for SQL and Layout Generation

Renders Jinja2 templates (.j2 files) with provided context variables.
Used to generate SQL files and layout JSONs dynamically.

Usage:
    # Render SQL template
    python src/template_renderer.py \
        --template templates/sql/diagnostic_daily.sql.j2 \
        --output layouts/game_health_check/sql/diagnostic_daily_fw2.sql \
        --var game_id=fw2 \
        --var log_date=2025-01-15
    
    # Render layout template
    python src/template_renderer.py \
        --template templates/layouts/etl_layout.json.j2 \
        --output layouts/game_health_check/etl/active_details.json \
        --vars-file configs/etl_active_details.yaml
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

try:
    from jinja2 import Environment, FileSystemLoader, TemplateNotFound, select_autoescape
    import yaml
except ImportError:
    print("Error: jinja2 and pyyaml are required. Install them:")
    print("  pip install jinja2 pyyaml")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TemplateRenderer:
    """Renders Jinja2 templates for SQL and layout generation"""
    
    def __init__(self, template_dir: str = 'templates'):
        """
        Initialize the template renderer
        
        Args:
            template_dir: Directory containing Jinja2 templates
        """
        self.template_dir = Path(template_dir)
        
        # Create Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True
        )
        
        # Add custom filters
        self.env.filters['quote'] = lambda s: f"'{s}'"
        self.env.filters['upper'] = str.upper
        self.env.filters['lower'] = str.lower
        
    def render_template(
        self,
        template_path: str,
        context: Dict[str, Any],
        output_path: str = None
    ) -> str:
        """
        Render a Jinja2 template
        
        Args:
            template_path: Path to template file (relative to template_dir)
            context: Dictionary of variables for template rendering
            output_path: Optional path to write rendered output
            
        Returns:
            Rendered template string
        """
        try:
            # Get template relative to template_dir
            template_file = str(Path(template_path).relative_to(self.template_dir))
        except ValueError:
            # Path is already relative
            template_file = template_path
            
        try:
            template = self.env.get_template(template_file)
        except TemplateNotFound:
            logger.error(f"Template not found: {template_file}")
            logger.error(f"Search path: {self.template_dir.absolute()}")
            raise
        
        # Render template
        rendered = template.render(**context)
        
        # Write to file if output path provided
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text(rendered, encoding='utf-8')
            logger.info(f"Rendered template to: {output_path}")
        
        return rendered
    
    def render_batch(
        self,
        template_path: str,
        contexts: List[Dict[str, Any]],
        output_dir: str,
        output_template: str = "{name}.sql"
    ):
        """
        Render template multiple times with different contexts
        
        Args:
            template_path: Path to template file
            contexts: List of context dictionaries
            output_dir: Directory for output files
            output_template: Template for output filename (can use context vars)
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for context in contexts:
            # Generate output filename from template
            output_name = output_template.format(**context)
            output_file = output_path / output_name
            
            # Render template
            self.render_template(template_path, context, str(output_file))
            
    def load_vars_from_file(self, vars_file: str) -> Dict[str, Any]:
        """
        Load template variables from YAML or JSON file
        
        Args:
            vars_file: Path to variables file (.yaml/.yml/.json)
            
        Returns:
            Dictionary of variables
        """
        file_path = Path(vars_file)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Variables file not found: {vars_file}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.suffix in ['.yaml', '.yml']:
                return yaml.safe_load(f) or {}
            elif file_path.suffix == '.json':
                return json.load(f)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")


def parse_cli_vars(var_args: List[str]) -> Dict[str, Any]:
    """
    Parse command-line variables in format key=value
    
    Args:
        var_args: List of "key=value" strings
        
    Returns:
        Dictionary of parsed variables
    """
    vars_dict = {}
    
    for var in var_args:
        if '=' not in var:
            logger.warning(f"Skipping invalid variable format: {var}")
            continue
            
        key, value = var.split('=', 1)
        
        # Try to parse as JSON for complex types
        try:
            vars_dict[key] = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            # Treat as string
            vars_dict[key] = value
    
    return vars_dict


def main():
    parser = argparse.ArgumentParser(
        description='Render Jinja2 templates for SQL and layouts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Render SQL with CLI variables
  python src/template_renderer.py \\
      --template templates/sql/diagnostic_daily.sql.j2 \\
      --output output.sql \\
      --var game_id=fw2 --var log_date=2025-01-15
  
  # Render layout with variables file
  python src/template_renderer.py \\
      --template templates/layouts/etl_layout.json.j2 \\
      --output layouts/etl/custom.json \\
      --vars-file configs/layout_vars.yaml
  
  # Render multiple SQL files for different games
  python src/template_renderer.py \\
      --template templates/sql/diagnostic_daily.sql.j2 \\
      --batch configs/games_list.yaml \\
      --output-dir sql_generated \\
      --output-template "{game_id}_diagnostic.sql"
        """
    )
    
    parser.add_argument(
        '-t', '--template',
        required=True,
        help='Path to Jinja2 template file (relative to templates/)'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output file path'
    )
    
    parser.add_argument(
        '--template-dir',
        default='templates',
        help='Directory containing templates (default: templates/)'
    )
    
    parser.add_argument(
        '--var',
        action='append',
        dest='vars',
        default=[],
        help='Template variable in format key=value (can specify multiple times)'
    )
    
    parser.add_argument(
        '--vars-file',
        help='YAML or JSON file containing template variables'
    )
    
    parser.add_argument(
        '--batch',
        help='YAML/JSON file with list of contexts for batch rendering'
    )
    
    parser.add_argument(
        '--output-dir',
        help='Output directory for batch rendering'
    )
    
    parser.add_argument(
        '--output-template',
        default='{name}.sql',
        help='Output filename template for batch (default: {name}.sql)'
    )
    
    parser.add_argument(
        '--print',
        action='store_true',
        help='Print rendered output to stdout instead of writing to file'
    )
    
    args = parser.parse_args()
    
    # Initialize renderer
    renderer = TemplateRenderer(template_dir=args.template_dir)
    
    # Load variables
    context = {}
    
    if args.vars_file:
        context.update(renderer.load_vars_from_file(args.vars_file))
    
    if args.vars:
        context.update(parse_cli_vars(args.vars))
    
    # Batch rendering
    if args.batch:
        if not args.output_dir:
            logger.error("--output-dir required for batch rendering")
            sys.exit(1)
        
        batch_data = renderer.load_vars_from_file(args.batch)
        
        if isinstance(batch_data, dict) and 'contexts' in batch_data:
            contexts = batch_data['contexts']
        elif isinstance(batch_data, list):
            contexts = batch_data
        else:
            logger.error("Batch file must contain 'contexts' key or be a list")
            sys.exit(1)
        
        renderer.render_batch(
            args.template,
            contexts,
            args.output_dir,
            args.output_template
        )
        
        logger.info(f"Rendered {len(contexts)} files to {args.output_dir}")
        return
    
    # Single rendering
    output_path = None if args.print else args.output
    
    rendered = renderer.render_template(
        args.template,
        context,
        output_path
    )
    
    if args.print:
        print(rendered)


if __name__ == '__main__':
    main()
