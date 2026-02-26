"""
Test script to verify sqlTemplate rendering and execution
"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from etl_engine import render_sql_template

# Test variables
variables = {
    'gameId': 'gnoth',
    'logDate': '2026-01-06'
}

# Test active.sql.j2
template_path = 'templates/rolling_forecast/active.sql.j2'
rendered_query = render_sql_template(template_path, variables)

print("=" * 80)
print("RENDERED SQL QUERY:")
print("=" * 80)
print(rendered_query)
print("=" * 80)

# Check if variables were substituted
if '{{ gameId }}' in rendered_query or '{{ logDate }}' in rendered_query:
    print("\n❌ ERROR: Variables not substituted!")
else:
    print("\n✅ SUCCESS: Variables substituted correctly")

# Check if query has GROUP BY
if 'GROUP BY' in rendered_query:
    print("✅ SUCCESS: Query contains GROUP BY clause")
else:
    print("❌ ERROR: Query missing GROUP BY clause")

# Check if country filter exists
if "country_code != 'na'" in rendered_query:
    print("✅ SUCCESS: Country filter present")
else:
    print("❌ WARNING: Country filter missing")
