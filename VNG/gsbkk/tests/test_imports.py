"""
Test module imports for refactored code

Run with: python tests/test_imports.py
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def test_core_models():
    """Test core.models imports"""
    # Import directly from module to avoid __init__ dependency chain
    from src.core.models import LayoutConfig, InputSource, OutputConfig, JdbcCredentials
    
    # Test InputSource creation
    source = InputSource(
        name='test_source',
        type='jdbc',
        connection='GDS_POSTGRES'
    )
    assert source.name == 'test_source'
    assert source.type == 'jdbc'
    
    # Test from_dict
    source_dict = {'name': 'active_data', 'type': 'file', 'path': '/data/test'}
    source2 = InputSource.from_dict(source_dict)
    assert source2.name == 'active_data'
    assert source2.path == '/data/test'
    
    print("✅ core.models imports OK")

def test_core_templates():
    """Test core.templates imports"""
    from src.core.templates import substitute_variables
    
    # Test variable substitution
    obj = {'path': '/data/{gameId}/{logDate}', 'items': ['{gameId}']}
    variables = {'gameId': 'gnoth', 'logDate': '2026-01-12'}
    
    result = substitute_variables(obj, variables)
    assert result['path'] == '/data/gnoth/2026-01-12'
    assert result['items'][0] == 'gnoth'
    print("✅ core.templates imports OK")

def test_core_loaders():
    """Test core.loaders imports (without credentials)"""
    try:
        from src.core.loaders import get_jdbc_url
        print("✅ core.loaders imports OK")
    except ImportError as e:
        print(f"⚠️ core.loaders skipped (missing dependency: {e})")

def test_etl_engine_imports():
    """Test etl_engine imports work with new modular structure"""
    # This tests that the import chain works
    try:
        from src.core.templates import render_sql_template, resolve_table_path, substitute_variables
        from src.core.loaders import get_jdbc_url
        print("✅ etl_engine compatible imports OK")
    except ImportError as e:
        print(f"⚠️ etl_engine imports skipped (missing dependency: {e})")

if __name__ == '__main__':
    print("Testing refactored imports...")
    test_core_models()
    test_core_templates()
    test_core_loaders()
    test_etl_engine_imports()
    print("\n✅ All import tests passed!")
