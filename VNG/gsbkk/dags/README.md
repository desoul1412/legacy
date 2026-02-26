# GSBKK DAGs

Airflow DAG organization and development guide.

## Directory Structure

```
dags/
├── game_health_check/          # Game health monitoring DAGs
│   ├── gnoth_game_health_check_dag.py
│   ├── l2m_game_health_check_dag.py
│   └── pwmsea_game_health_check_dag.py
├── rolling_forecast/           # Revenue forecasting DAGs
│   ├── gsheet_rfc_import_dag.py      # Google Sheets → PostgreSQL
│   ├── gnoth_rolling_forecast_dag.py
│   ├── l2m_rolling_forecast_dag.py
│   └── pwmsea_rolling_forecast_dag.py
├── sensortower/                # Market research DAG
│   └── sensortower_dag.py
├── re_standardization/         # Legacy migration DAG
├── currency_mapping_dag.py     # Currency conversion rates
├── utils/                      # Helper functions
│   ├── dag_helpers.py         # Task factory functions
│   └── __init__.py
└── README.md                   # This file
```

## DAG Categories

### 1. Game Health Check
**Purpose**: Daily KPI monitoring  
**Schedule**: Daily (3-7 AM per game)  
**Pattern**: ETL → STD → CONS stages

| DAG | Game | Schedule | Market |
|-----|------|----------|--------|
| `game_health_check_gnoth` | Gunny Origin TH | 3 AM | Single (TH) |
| `game_health_check_l2m` | Lineage 2 Mobile | 6 AM | Multi-country |
| `game_health_check_pwmsea` | Perfect World Mobile | 7 AM | SEA |

### 2. Rolling Forecast
**Purpose**: Revenue predictions and actual data import  
**Schedule**: Daily (varies)

| DAG | Purpose | Schedule |
|-----|---------|----------|
| `rolling_forecast_gsheet_import` | Import from Google Sheets | 9 AM |
| `gnoth_daily_actual_rfc` | Generate forecasts | After health check |
| `l2m_daily_actual_rfc` | Generate forecasts | After health check |
| `pwmsea_daily_actual_rfc` | Generate forecasts | After health check |

### 3. Market Research
**Purpose**: SensorTower competitive intelligence

| DAG | Schedule | Markets |
|-----|----------|---------|
| `sensortower_market_research` | 1st of month, 2 AM | VN, TH, US |

### 4. Utilities
| DAG | Purpose | Schedule |
|-----|---------|----------|
| `currency_mapping` | Update exchange rates | Daily |

## Helper Functions

Located in [`utils/dag_helpers.py`](utils/dag_helpers.py):

### create_etl_operator()
Creates BashOperator for ETL tasks using `run_etl_process.sh`.

```python
from utils.dag_helpers import create_etl_operator

task = create_etl_operator(
    dag=dag,
    task_id='import_daily_data',
    layout='layouts/path/to/layout.json',
    game_id='gnoth',
    log_date='{{ ds }}'
)
```

### create_pipeline_tasks()
Batch creates tasks from declarative configuration.

```python
from utils.dag_helpers import create_pipeline_tasks

pipeline = [
    {'name': 'etl_active', 'type': 'etl', 
     'layout': 'layouts/game_health_check/gnoth/etl/active.json'},
    {'name': 'std_active', 'type': 'etl',
     'layout': 'layouts/game_health_check/gnoth/std/active.json'},
]

tasks = create_pipeline_tasks(dag, pipeline, game_id='gnoth')

# Use tasks
tasks['etl_active'] >> tasks['std_active']
```

## DAG Patterns

### Pattern 1: Linear Pipeline
```python
start >> etl >> std >> cons >> end
```

### Pattern 2: Parallel ETL
```python
start >> [etl_active, etl_charge, etl_campaign] >> etl_done
etl_done >> std >> cons >> end
```

### Pattern 3: Trigger Downstream
```python
cons_complete >> TriggerDagRunOperator(
    task_id='trigger_rfc',
    trigger_dag_id='gnoth_daily_actual_rfc'
)
```

## Airflow Variables

Common Jinja2 variables used:
- `{{ ds }}` - Data interval start (YYYY-MM-DD)
- `{{ execution_date }}` - Same as ds (legacy)
- `{{ macros.ds_add(ds, -1) }}` - Previous day

## Best Practices

1. **Use helper functions** - Don't create BashOperators manually
2. **Declarative pipelines** - Use `create_pipeline_tasks()` for consistency
3. **Catchup wisely** - Set `catchup=True` for game health, `False` for imports
4. **Max active runs=1** - Prevents concurrent runs corrupting data
5. **Retry logic** - Use 1-2 retries with 5-10 min delay
6. **Tags** - Always add relevant tags for filtering in UI

## Related Documentation

- [Game Health Check](../layouts/game_health_check/README.md)
- [Rolling Forecast](../layouts/rolling_forecast/README.md)
- [SensorTower](../layouts/sensortower/README.md)
- [DAG Development Guide](../docs/DAG_DEVELOPMENT_GUIDE.md)
