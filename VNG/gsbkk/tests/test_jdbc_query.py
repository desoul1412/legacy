"""
Test to verify JDBC query with GROUP BY is executed on database
This simulates what etl_engine.py does
"""

# Expected behavior:
# When Spark reads from JDBC with a subquery like:
#   spark.read.jdbc(url=jdbc_url, table="(SELECT ... GROUP BY ...) AS source_data")
# 
# The GROUP BY should execute ON THE DATABASE, not in Spark
# Result: Should return aggregated data (1 row per country)

query = """
SELECT 
    report_date,
    country_code,
    SUM("DAU") as dau,
    SUM("DPU") as pu,
    SUM("NPU") as npu,
    SUM("NRU") as nru,
    SUM("Cost") as cost,
    SUM("Installs") as installs
FROM public.ghc_diagnostic_daily_gnoth
WHERE report_date = DATE '2026-01-06'
  AND country_code != 'na'
GROUP BY report_date, country_code
"""

table = f"({query}) AS source_data"

print("=" * 80)
print("JDBC TABLE PARAMETER:")
print("=" * 80)
print(table)
print("=" * 80)
print()
print("Expected behavior:")
print("✅ Query executes on Postgres database")
print("✅ GROUP BY aggregation happens on database")
print("✅ Spark receives already-aggregated data")
print("✅ Result: 1 row per (report_date, country_code)")
print()
print("If this is NOT happening:")
print("❌ Query might be loading full table then aggregating in Spark")
print("❌ Check Spark query plan in logs")
print("❌ Verify JDBC pushdown is enabled")
