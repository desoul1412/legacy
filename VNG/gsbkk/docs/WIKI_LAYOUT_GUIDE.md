# Layout Files - Complete Guide

**Purpose:** Layout files define the ETL pipeline structure in JSON format. They specify input sources, transformations, and output destinations for data processing.

---

## 📋 Table of Contents
1. [Overview](#overview)
2. [Layout File Structure](#layout-file-structure)
3. [Input Sources (`inputSources`)](#input-sources-inputsources)
4. [Output Destinations (`outputs`)](#output-destinations-outputs)
5. [Variables & Templating](#variables--templating)
6. [SQL Templates](#sql-templates)
7. [Schemas & Transformations](#schemas--transformations)
8. [Spark Configuration](#spark-configuration)
9. [Complete Examples](#complete-examples)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Overview

### What is a Layout File?

A **layout file** is a JSON configuration that defines:
- **Where to read data from** (`inputSources`)
- **How to transform it** (`sqlFile`, `schemas`)
- **Where to write results** (`outputs`)
- **Runtime variables** (`variables`, `sparkConfig`)

**Example workflow:**
```
Layout JSON → ETL Engine → Read Data → Transform → Write Data
```

### Why Use Layouts?

✅ **Declarative:** Define "what" not "how" - engine handles execution  
✅ **Reusable:** Same layout works for multiple games/dates with variables  
✅ **Version Controlled:** JSON files track changes easily  
✅ **Testable:** Validate without running full pipeline  
✅ **Composable:** Chain layouts in DAGs for complex workflows  

---

## Layout File Structure

### Standard Format (Required)

All layout files **MUST** follow this structure:

```json
{
  "description": "Human-readable description of this pipeline step",
  "comment": "Optional detailed explanation of the transformation logic",
  
  "inputSources": [
    { /* input source 1 */ },
    { /* input source 2 */ }
  ],
  
  "outputs": [
    { /* output destination */ }
  ]
}
```

### Optional Fields

```json
{
  "sqlFile": "path/to/template.sql.j2",
  "variables": {
    "custom_var": "value"
  },
  "sparkConfig": {
    "spark.sql.shuffle.partitions": "200"
  }
}
```

**Field Reference:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `description` | String | ✅ Yes | Short title (shown in logs) |
| `comment` | String | ⚪ Optional | Detailed explanation |
| `inputSources` | Array | ✅ Yes | List of data sources to read |
| `outputs` | Array | ✅ Yes | List of output destinations |
| `sqlFile` | String | ⚪ Optional | Jinja2 SQL template for transformations |
| `variables` | Object | ⚪ Optional | Custom variables for templating |
| `sparkConfig` | Object | ⚪ Optional | Spark runtime configuration |

---

## Input Sources (`inputSources`)

### Overview

`inputSources` is an **array** of objects defining where to read data. Each source becomes a named DataFrame in Spark.

### Input Source Types

#### 1. File Input (HDFS/Parquet/JSON/CSV)

```json
{
  "name": "active_users",
  "type": "file",
  "path": "hdfs://c0s/user/gsbkk/data/{gameId}/{logDate}",
  "format": "parquet"
}
```

**Parameters:**

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `name` | ✅ Yes | DataFrame name (used in SQL/schemas) | `"active_users"` |
| `type` | ✅ Yes | Must be `"file"` | `"file"` |
| `path` | ✅ Yes | HDFS/local path (supports variables) | `"hdfs://c0s/..."` |
| `format` | ✅ Yes | Data format | `"parquet"`, `"json"`, `"csv"` |
| `options` | ⚪ Optional | Spark read options | `{"header": "true"}` |

**Supported Formats:**
- **Parquet** (recommended for performance)
- **JSON** (flexible schema)
- **CSV** (legacy data)
- **ORC** (Hive compatibility)

#### 2. JDBC Input (Postgres/Trino/MySQL)

```json
{
  "name": "user_profile",
  "type": "jdbc",
  "connection": "GDS_POSTGRES",
  "table": "(SELECT * FROM fw2.user_profile WHERE ds = '{logDate}') as user_profile"
}
```

**Parameters:**

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `name` | ✅ Yes | DataFrame name | `"user_profile"` |
| `type` | ✅ Yes | Must be `"jdbc"` | `"jdbc"` |
| `connection` | ✅ Yes | Connection name from config | `"GDS_POSTGRES"`, `"TSN_POSTGRES"` |
| `table` | ✅ Yes | Table name or SQL subquery | `"schema.table"` or `"(SELECT ...) as alias"` |

**Connection Names (defined in `configs/data_path.yaml`):**
- `GDS_POSTGRES` - Game database (source)
- `TSN_POSTGRES` - Analytics database (staging/output)
- `TRINO_ICEBERG` - Data lake queries

**Important:** When using subqueries, **always include an alias** (`as alias_name`)

#### 3. Trino Input (Iceberg Tables)

```json
{
  "name": "recharge_data",
  "type": "jdbc",
  "connection": "TRINO_ICEBERG",
  "table": "(SELECT * FROM iceberg.l2m.etl_recharge WHERE ds = '{logDate}') as recharge"
}
```

### Multiple Input Sources

You can define multiple sources - each becomes a separate DataFrame:

```json
{
  "inputSources": [
    {
      "name": "active",
      "type": "file",
      "path": "hdfs://c0s/.../active/{gameId}/{logDate}",
      "format": "parquet"
    },
    {
      "name": "charge",
      "type": "file",
      "path": "hdfs://c0s/.../charge/{gameId}/{logDate}",
      "format": "parquet"
    },
    {
      "name": "currency_mapping",
      "type": "file",
      "path": "hdfs://c0s/.../currency_mapping",
      "format": "parquet"
    }
  ]
}
```

**Access in SQL:**
```sql
SELECT a.user_id, c.revenue
FROM active a
JOIN charge c ON a.user_id = c.user_id
JOIN currency_mapping cm ON c.currency_code = cm.currency_code
```

### Variable Substitution in Paths

Paths support **runtime variable substitution** with `{variableName}`:

```json
{
  "path": "hdfs://c0s/user/workspace/{gameId}/data/{logDate}"
}
```

**Common Variables:**
- `{gameId}` - Game identifier (e.g., `fw2`, `l2m`, `slth`)
- `{logDate}` - Report date (e.g., `2026-01-07`)
- `{market}` - Market code (e.g., `TH`, `VN`, `SEA`)

**Usage:**
```bash
./run_etl_process.sh --layout layout.json --vars "gameId=fw2,logDate=2026-01-07"
```

---

## Output Destinations (`outputs`)

### Overview

`outputs` is an **array** of objects defining where to write processed data. Usually contains one output, but supports multiple destinations.

### Output Types

#### 1. File Output (HDFS/Parquet)

```json
{
  "type": "file",
  "path": "hdfs://c0s/user/gsbkk/output/{gameId}/{logDate}",
  "format": "parquet",
  "mode": "overwrite",
  "numPartitions": 4
}
```

**Parameters:**

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `type` | ✅ Yes | Must be `"file"` | `"file"` |
| `path` | ✅ Yes | Output path (supports variables) | `"hdfs://c0s/..."` |
| `format` | ✅ Yes | Output format | `"parquet"`, `"json"`, `"csv"` |
| `mode` | ✅ Yes | Write mode | `"overwrite"`, `"append"` |
| `numPartitions` | ⚪ Optional | Number of output files | `4`, `8`, `16` |
| `partitionBy` | ⚪ Optional | Partition columns | `["country_code", "ds"]` |

**Write Modes:**
- `"overwrite"` - Replace all data in path
- `"append"` - Add to existing data (use with partitioning)

#### 2. JDBC Output (Postgres)

```json
{
  "type": "jdbc",
  "connection": "TSN_POSTGRES",
  "table": "public.ghc_diagnostic_daily_{gameId}",
  "mode": "overwrite",
  "deleteCondition": "report_date = '{logDate}'"
}
```

**Parameters:**

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `type` | ✅ Yes | Must be `"jdbc"` | `"jdbc"` |
| `connection` | ✅ Yes | Connection name | `"TSN_POSTGRES"` |
| `table` | ✅ Yes | Target table (supports variables) | `"public.table_{gameId}"` |
| `mode` | ✅ Yes | Write mode | `"overwrite"`, `"append"` |
| `deleteCondition` | ⚪ Optional | SQL WHERE clause for deletion | `"ds = '{logDate}'"` |

**Delete Condition:**
- Used with `mode: "overwrite"` to delete specific partitions before inserting
- Prevents full table replacement - only removes matching rows
- Example: `"report_date = '{logDate}' AND game_id = '{gameId}'"`

#### 3. Schema Definitions in Outputs

You can define schema transformations directly in outputs:

```json
{
  "outputs": [{
    "type": "file",
    "path": "hdfs://c0s/.../output/{logDate}",
    "format": "parquet",
    "mode": "overwrite",
    "schemas": [{
      "comment": "Transform columns",
      "columns": [
        {"colName": "user_id", "colType": "STRING"},
        {"colName": "revenue", "colType": "DOUBLE"},
        {"colName": "report_date", "colType": "DATE"}
      ]
    }]
  }]
}
```

---

## Variables & Templating

### Runtime Variables

Variables are passed at runtime and substituted in layout files:

**Command:**
```bash
./run_etl_process.sh --layout layout.json --vars "gameId=fw2,logDate=2026-01-07"
```

**Layout:**
```json
{
  "inputSources": [{
    "path": "hdfs://c0s/data/{gameId}/{logDate}"
  }]
}
```

**Result:**
```json
{
  "inputSources": [{
    "path": "hdfs://c0s/data/fw2/2026-01-07"
  }]
}
```

### Custom Variables

Define custom variables in layout for SQL templates:

```json
{
  "variables": {
    "market_type": "single",
    "country_code": "TH",
    "vip_threshold": 25840
  },
  "sqlFile": "templates/forecast.sql.j2"
}
```

**Usage in SQL template:**
```jinja
SELECT * 
FROM users 
WHERE country_code = '{{ country_code }}'
  AND market = '{{ market_type }}'
```

### Variable Scope

Variables are available in:
1. **Layout paths** - `{gameId}`, `{logDate}`
2. **SQL templates** - `{{ game_id }}`, `{{ log_date }}`
3. **Schema definitions** - `{gameId}` in column expressions

**Naming Convention:**
- Layout JSON: `camelCase` (`gameId`, `logDate`)
- SQL templates: `snake_case` (`game_id`, `log_date`)
- ETL engine auto-converts between conventions

---

## SQL Templates

### Using SQL Files

Instead of defining transformations in `schemas`, use SQL template files:

**Layout:**
```json
{
  "inputSources": [ /* sources */ ],
  "sqlFile": "templates/game_health_check/diagnostic_daily.sql.j2",
  "outputs": [ /* outputs */ ]
}
```

**SQL Template (`diagnostic_daily.sql.j2`):**
```sql
{% import 'macros.sql' as macros %}

SELECT 
    user_id,
    {{ macros.calculate_vip('total_rev') }} AS vip_level,
    revenue
FROM active
WHERE report_date = '{{ log_date }}'
```

### Template Search Paths

ETL engine searches for templates in:
1. Template's own directory
2. `templates/sql/` (for macros)
3. `templates/` (root)

**Example:**
```
templates/
├── sql/
│   └── macros.sql              # Shared macros
├── game_health_check/
│   ├── diagnostic_daily.sql.j2
│   └── package_performance.sql.j2
└── rolling_forecast/
    └── rolling_forecast.sql.j2
```

---

## Schemas & Transformations

### Schema Structure

Define column transformations when NOT using SQL templates:

```json
{
  "outputs": [{
    "schemas": [{
      "comment": "Optional description",
      "columns": [
        {"colName": "user_id", "colType": "STRING"},
        {"colName": "total_revenue", "colType": "DOUBLE", "expression": "SUM(revenue)"}
      ]
    }]
  }]
}
```

### Column Definition

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `colName` | ✅ Yes | Output column name | `"user_id"` |
| `colType` | ✅ Yes | Data type | `"STRING"`, `"DOUBLE"`, `"DATE"` |
| `expression` | ⚪ Optional | SQL expression | `"SUM(revenue)"` |

**Data Types:**
- `STRING`, `INT`, `BIGINT`, `DOUBLE`, `FLOAT`
- `DATE`, `TIMESTAMP`
- `BOOLEAN`
- `ARRAY<type>`, `MAP<key,value>`

### Pass-Through Schema

To copy all columns without transformation:

```json
{
  "schemas": [{
    "comment": "Pass through all columns",
    "columns": [
      {"colName": "*"}
    ]
  }]
}
```

---

## Spark Configuration

### Spark Config Options

Tune Spark performance with `sparkConfig`:

```json
{
  "sparkConfig": {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g"
  }
}
```

**Common Settings:**

| Setting | Description | Default | Recommended |
|---------|-------------|---------|-------------|
| `spark.sql.shuffle.partitions` | Shuffle parallelism | 200 | 100-400 based on data size |
| `spark.sql.adaptive.enabled` | Adaptive query execution | false | `"true"` for large datasets |
| `spark.executor.memory` | Executor memory | 1g | 4g-8g based on cluster |
| `spark.driver.memory` | Driver memory | 1g | 2g-4g |

### When to Use sparkConfig

- **Large datasets** (>100GB) - increase partitions
- **Complex joins** - enable adaptive execution
- **Memory errors** - increase executor/driver memory
- **Slow shuffles** - tune shuffle partitions

---

## Complete Examples

### Example 1: Simple File Transformation

**Layout:** `layouts/sensortower/etl/new_games.json`

```json
{
  "description": "SensorTower: Raw New Games → ETL (Parquet)",
  "comment": "Converts raw new games JSON data to parquet format",
  
  "inputSources": [
    {
      "name": "source_data",
      "type": "file",
      "format": "json",
      "path": "hdfs://c0s/user/workspace/sensortower/raw/new_games/{logDate}"
    }
  ],
  
  "outputs": [{
    "type": "file",
    "path": "hdfs://c0s/user/workspace/sensortower/etl/new_games/{logDate}",
    "format": "parquet",
    "numPartitions": 4,
    "schemas": [{
      "comment": "Pass through all columns from raw JSON",
      "columns": [
        {"colName": "*"}
      ]
    }]
  }]
}
```

**Usage:**
```bash
./run_etl_process.sh \
  --layout layouts/sensortower/etl/new_games.json \
  --vars "logDate=2026-01-07"
```

### Example 2: Multi-Source JDBC Join with SQL Template

**Layout:** `layouts/game_health_check/cons/diagnostic_daily.json`

```json
{
  "description": "Game Health Check - Consolidate diagnostic daily metrics",
  "comment": "Combines active, charge, user_profile, and campaign data",
  
  "inputSources": [
    {
      "name": "user_profile",
      "type": "jdbc",
      "connection": "GDS_POSTGRES",
      "table": "(SELECT game_id, user_id, country_code, total_rev FROM {gameId}.user_profile) as user_profile"
    },
    {
      "name": "currency_mapping",
      "type": "file",
      "path": "hdfs://c0s/user/workspace/currency_mapping",
      "format": "parquet"
    },
    {
      "name": "charge_details",
      "type": "file",
      "path": "hdfs://c0s/user/workspace/game_health_check/std/charge/{gameId}/{logDate}",
      "format": "parquet"
    },
    {
      "name": "active_details",
      "type": "file",
      "path": "hdfs://c0s/user/workspace/game_health_check/std/active/{gameId}/{logDate}",
      "format": "parquet"
    }
  ],
  
  "sqlFile": "templates/game_health_check/diagnostic_daily.sql.j2",
  
  "outputs": [{
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.ghc_diagnostic_daily_{gameId}",
    "mode": "overwrite",
    "deleteCondition": "report_date = '{logDate}'"
  }]
}
```

**Usage:**
```bash
./run_etl_process.sh \
  --layout layouts/game_health_check/cons/diagnostic_daily.json \
  --vars "gameId=fw2,logDate=2026-01-07"
```

### Example 3: Parameterized Template with Custom Variables

**Layout:** `layouts/rolling_forecast/cons/single_market.json`

```json
{
  "description": "Rolling Forecast - Single market metrics",
  "comment": "For Thailand-only games (slth)",
  
  "inputSources": [
    {
      "name": "active",
      "type": "file",
      "path": "hdfs://c0s/.../rolling_forecast/etl/active/{gameId}/{logDate}",
      "format": "parquet"
    },
    {
      "name": "retention",
      "type": "file",
      "path": "hdfs://c0s/.../rolling_forecast/etl/retention/{gameId}/{logDate}",
      "format": "parquet"
    }
  ],
  
  "sqlFile": "templates/rolling_forecast/rolling_forecast_market.sql.j2",
  
  "variables": {
    "market_type": "single",
    "country_code": "TH"
  },
  
  "outputs": [{
    "type": "jdbc",
    "connection": "TSN_POSTGRES",
    "table": "public.rfc_output_{gameId}",
    "mode": "overwrite",
    "deleteCondition": "report_date = '{logDate}'"
  }]
}
```

**SQL Template uses variables:**
```jinja
{% if market_type == 'single' %}
    -- Single market logic
    WHERE country_code = '{{ country_code }}'
{% elif market_type == 'sea' %}
    -- SEA aggregate logic
    GROUP BY country_code
{% endif %}
```

---

## Best Practices

### 1. Naming Conventions

✅ **DO:**
- Layout files: `{stage}_{entity}.json` (e.g., `etl_active.json`, `cons_diagnostic_daily.json`)
- Input names: Descriptive nouns (`user_profile`, `charge_details`, `currency_mapping`)
- Output tables: `{project}_{table}_{gameId}` (e.g., `ghc_diagnostic_daily_fw2`)

❌ **DON'T:**
- Generic names (`data`, `input1`, `output`)
- Abbreviations (`usr`, `chg`, `curr`)
- Inconsistent casing

### 2. Variable Usage

✅ **DO:**
- Always use `{gameId}` and `{logDate}` for reusability
- Define custom variables for conditional logic
- Document variables in layout comments

❌ **DON'T:**
- Hardcode game IDs or dates in paths
- Mix variable naming conventions

### 3. SQL Templates vs Schemas

**Use SQL templates when:**
- Complex joins (3+ tables)
- Aggregations, window functions
- Conditional logic based on variables
- Reusable macros needed

**Use schemas when:**
- Simple pass-through (convert JSON → Parquet)
- Column renaming/type casting
- Single table transformations

### 4. Performance Optimization

✅ **DO:**
- Use Parquet format for intermediate/output data
- Set appropriate `numPartitions` (4-16 for most cases)
- Enable adaptive execution for large datasets
- Filter data early in SQL (WHERE clauses)

❌ **DON'T:**
- Read entire tables without filtering
- Use CSV for large datasets (slow parsing)
- Set shuffle partitions too high (>500) or too low (<50)

### 5. Error Handling

✅ **DO:**
- Add descriptive `description` and `comment` fields
- Test layouts with small date ranges first
- Validate JSON syntax before deployment
- Use `deleteCondition` to prevent data loss

❌ **DON'T:**
- Use `mode: "overwrite"` without `deleteCondition` (deletes entire table!)
- Deploy untested layouts to production
- Omit required fields

---

## Troubleshooting

### Common Errors

#### 1. JSON Syntax Error

**Error:**
```
Expected comma or closing bracket
```

**Fix:**
- Validate JSON with `python -m json.tool layout.json`
- Check for missing closing brackets `]`, `}`
- Ensure commas between array elements

#### 2. Variable Not Substituted

**Error:**
```
Path not found: hdfs://c0s/.../data/{gameId}/{logDate}
```

**Fix:**
- Pass variables in command: `--vars "gameId=fw2,logDate=2026-01-07"`
- Check variable names match exactly (case-sensitive)

#### 3. Table/Path Not Found

**Error:**
```
Table or view not found: user_profile
```

**Fix:**
- Verify connection name is correct (`GDS_POSTGRES`, `TSN_POSTGRES`)
- Check table name in JDBC source
- Ensure HDFS path exists for file inputs

#### 4. Schema Mismatch

**Error:**
```
Cannot write to table: column 'revenue' has type DOUBLE but data has type STRING
```

**Fix:**
- Cast columns in SQL: `CAST(revenue AS DOUBLE)`
- Update schema definition `colType` to match source
- Use SQL template for explicit type conversions

#### 5. Out of Memory

**Error:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Fix:**
- Add `sparkConfig`:
  ```json
  {
    "sparkConfig": {
      "spark.executor.memory": "8g",
      "spark.driver.memory": "4g",
      "spark.sql.shuffle.partitions": "200"
    }
  }
  ```
- Reduce date range for testing
- Increase cluster resources

### Validation Checklist

Before deploying a layout:

- [ ] JSON syntax is valid (`python -m json.tool layout.json`)
- [ ] All required fields present (`inputSources`, `outputs`, `description`)
- [ ] Input/output paths use variables (`{gameId}`, `{logDate}`)
- [ ] SQL template path is correct and file exists
- [ ] Connection names match config (`GDS_POSTGRES`, `TSN_POSTGRES`)
- [ ] JDBC subqueries have aliases (`as alias_name`)
- [ ] Test with small date range first
- [ ] `deleteCondition` used with JDBC outputs (prevents full table overwrite)

---

## Next Steps

📚 **Related Documentation:**
- [SQL Template Guide](WIKI_SQL_TEMPLATE_GUIDE.md) - Jinja2 templates and macros
- [Re-standardization Guide](WIKI_RESTANDARDIZATION_GUIDE.md) - Adding new games
- [ETL Engine Reference](../src/etl_engine.py) - Source code

🎯 **Quick Actions:**
- Create your first layout: Copy `layouts/sensortower/etl/new_games.json`
- Test a layout: `./run_etl_process.sh --layout your_layout.json --vars "gameId=test,logDate=2026-01-07"`
- Validate JSON: `python -m json.tool layouts/your_layout.json`

---

**Last Updated:** 2026-01-07  
**Maintainer:** Data Engineering Team
