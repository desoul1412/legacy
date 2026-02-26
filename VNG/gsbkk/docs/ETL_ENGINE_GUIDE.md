# ETL Engine Guide

Complete guide to the GSBKK ETL engine - the core framework for data pipeline execution.

## Overview

The ETL engine is a layout-driven framework that executes data pipelines defined in JSON layout files.

**Key Script**: [`run_etl_process.sh`](../run_etl_process.sh)  
**Engine**: [`src/etl_engine.py`](../src/etl_engine.py)

## Architecture

```
Layout JSON → ETL Engine → Spark → Output
   ↓              ↓          ↓        ↓
Config      Parse/Render  Execute   Write
```

### Execution Flow

1. **Parse layout** - Load JSON, validate structure
2. **Load inputs** - Read from JDBC/files/Google Sheets
3. **Render SQL** - Apply Jinja2 templates with variables
4. **Execute** - Run SQL in Spark, create DataFrame
5. **Write outputs** - Write to HDFS/PostgreSQL/Google Sheets

## Layout File Format

### Minimal Layout

```json
{
  "description": "Human-readable description",
  "inputSources": [{
    "name": "source_data",
    "type": "jdbc",
    "connection": "GDS_POSTGRES",
    "sqlTemplate": "templates/query.sql.j2"
  }],
  "outputs": [{
    "type": "file",
    "path": "hdfs://c0s/output/{logDate}",
    "format": "parquet"
  }]
}
```

### Full Layout

```json
{
  "description": "Process description",
  "comment": "Optional additional notes",
  
  "inputSources": [
    {
      "name": "temp_view_name",
      "type": "jdbc | file | gsheet",
      "connection": "GDS_POSTGRES | TSN_POSTGRES | GDS_TRINO",
      "sqlTemplate": "templates/path/query.sql.j2",
      "tableKey": "daily_active",
      "path": "hdfs://path/to/input",
      "format": "parquet | json | csv",
      "sheet_id": "1ABC...xyz",
      "range": "Sheet1!A:Z"
    }
  ],
  
  "sqlTemplate": "templates/path/transform.sql.j2",
  
  "outputs": [
    {
      "type": "file | jdbc | gsheet",
      "path": "hdfs://path/to/output/{logDate}",
      "format": "parquet",
      "numPartitions": 4,
      "mode": "overwrite | append",
      "connection": "TSN_POSTGRES",
      "table": "public.table_name",
      "deleteCondition": "date = '{logDate}'",
      "add_updated_at": true
    }
  ]
}
```

## Input Sources

### JDBC (Database)

```json
{
  "name": "active_data",
 "type": "jdbc",
  "connection": "GDS_POSTGRES",
  "sqlTemplate": "templates/query.sql.j2"
}
```

**Connections**:
- `GDS_POSTGRES` - Game data source
- `GDS_TRINO` - Data lake
- `TSN_POSTGRES` - Analytics database

### File (HDFS/Local)

```json
{
  "name": "parquet_data",
  "type": "file",
  "path": "hdfs://c0s/data/{gameId}/{logDate}",
  "format": "parquet"
}
```

**Formats**: `parquet`, `json`, `csv`

### Google Sheets

```json
{
  "name": "gsheet_data",
  "type": "gsheet",
  "sheet_id": "1ABC...xyz",
  "range": "Sheet1!A2:Z"
}
```

See [Google Sheets Integration Guide](GOOGLE_SHEETS_INTEGRATION.md) for details.

## SQL Templates

Templates use Jinja2 syntax:

```sql
SELECT 
    role_id,
    {{ calculate_vip_level('total_revenue') }} as vip_level,
    CAST('{{ logDate }}' AS DATE) as log_date
FROM {{ table_path }}
WHERE report_date = '{{ logDate }}'
```

**Variables Available**:
- `{logDate}` - Date argument
- `{gameId}` - Game ID argument
- `{table_path}` - Resolved table name from tableKey
- Custom variables from `--vars` argument

## Outputs

### File Output (HDFS)

```json
{
  "type": "file",
  "path": "hdfs://c0s/output/{gameId}/{logDate}",
  "format": "parquet",
  "mode": "overwrite",
  "numPartitions": 4
}
```

### JDBC Output (PostgreSQL)

```json
{
  "type": "jdbc",
  "connection": "TSN_POSTGRES",
  "table": "public.ghc_diagnostic_daily",
  "mode": "overwrite",
  "deleteCondition": "log_date = '{logDate}'",
  "add_updated_at": true
}
```

**Modes**:
- `overwrite` - Replace all data
- `append` - Add to existing data
- `append` + `deleteCondition` - Delete then insert (idempotent)

### Google Sheets Output

```json
{
  "type": "gsheet",
  "sheet_id": "1ABC...xyz",
  "range": "Output!A1:Z",
  "update_mode": "overwrite | append"
}
```

## Usage

### Basic Execution

```bash
./run_etl_process.sh \
    --layout layouts/path/to/layout.json \
    --gameId gnoth \
    --logDate 2026-01-20
```

### With Custom Variables

```bash
./run_etl_process.sh \
    --layout layouts/path/to/layout.json \
    --gameId gnoth \
    --logDate 2026-01-20 \
    --vars "market=TH,category=action"
```

### Advanced Options

```bash
./run_etl_process.sh \
    --layout layouts/path/to/layout.json \
    --gameId gnoth \
    --logDate 2026-01-20 \
    --spark-master yarn \
    --executor-memory 4g \
    --num-executors 8
```

## Variable Substitution

Variables in `{curly braces}` are replaced:

| Variable | Source | Example |
|----------|--------|---------|
| `{gameId}` | `--gameId` | `gnoth` |
| `{logDate}` | `--logDate` | `2026-01-20` |
| `{table_path}` | Resolved from tableKey | `common_tables_2.daily_role_active` |
| Custom | `--vars` | `{market}`, `{category}` |

## Idempotent Updates

Use `deleteCondition` for safe re-runs:

```json
{
  "outputs": [{
    "type": "jdbc",
    "table": "public.rfc_daily",
    "mode": "append",
    "deleteCondition": "game = '{gameId}' AND date = DATE('{logDate}')"
  }]
}
```

**How it works**:
1. Write transformed data to temp table
2. BEGIN transaction
3. DELETE FROM target WHERE deleteCondition
4. INSERT INTO target FROM temp_table
5. COMMIT (or ROLLBACK on error)

## Best Practices

1. **Test with small data** - Use date range limits
2. **Partition outputs** - Set `numPartitions` based on data volume
3. **Use deleteCondition** - For idempotent PostgreSQL writes
4. **Filter early** - In input SQL templates, not output
5. **Explicit types** - Cast Google Sheets data explicitly
6. **Add comments** - Describe purpose in layout files
7. **Validate JSON** - Use JSON linter before committing

## Troubleshooting

### Layout Not Found

**Error**: `Layout file not found: ...`

**Solution**: Use absolute path or path relative to repo root

### Template Rendering Error

**Error**: `TemplateNotFound: ...`

**Solution**: Check `sqlTemplate` path is relative to repo root

### Variable Not Defined

**Error**: `'logDate' is undefined`

**Solution**: Pass required variables via command line

### JDBC Connection Failed

**Error**: `Connection refused`

**Solution**: Check connection name in layout matches credentials

### Parquet Schema Mismatch

**Error**: `Parquet column 'x' type mismatch`

**Solution**: Delete old parquet files or use different output path

## Examples

See working examples:
- [Rolling Forecast](../layouts/rolling_forecast/)
- [Game Health Check](../layouts/game_health_check/)
- [SensorTower](../layouts/sensortower/)

## Related Documentation

- [Google Sheets Integration](GOOGLE_SHEETS_INTEGRATION.md)
- [SQL Template Guide](WIKI_SQL_TEMPLATE_GUIDE.md)
- [DAG Development](DAG_DEVELOPMENT_GUIDE.md)
