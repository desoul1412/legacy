# Re-Standardization

Legacy game data migration project - transforming old data formats to standard GSBKK schema.

## Overview

Re-standardization migrates non-standard game data formats to the unified analytics schema, enabling:
- Consistent reporting across all games
- Reuse of standard SQL templates
- Integration with game health check pipelines

**Current Games**: L2M (Lineage 2 Mobile)

## Architecture

```
Legacy Tables → ETL → Standardized Format → TSN Postgres
(old schema)          (transform)          (standard schema)
```

## Layouts

Located in [`l2m/`](l2m/) subdirectory:

### ETL Stage
- `etl/active.json` - Daily active users
- `etl/recharge.json` - Payment transactions
- `etl/item_gain.json` - Item acquisitions
- `etl/item_spend.json` - Item consumption
- `etl/money_gain.json` - Currency gains
- `etl/money_spend.json` - Currency spending

### CONS Stage
- `cons/l2m_active.json` - Standardized active table
- `cons/l2m_recharge.json` - Standardized payment table
- `cons/l2m_cumulative_revenue.json` - Revenue aggregation
- `cons/l2m_item_gain.json` - Item flow analysis
- `cons/l2m_server_performance.json` - Server metrics

## Usage

```bash
# Run ETL for specific metric
./run_etl_process.sh \
    --layout layouts/re-standardization/l2m/etl/recharge.json \
    --gameId l2m \
    --logDate 2026-01-20

# Run CONS to write to PostgreSQL
./run_etl_process.sh \
    --layout layouts/re-standardization/l2m/cons/l2m_recharge.json \
    --gameId l2m \
    --logDate 2026-01-20
```

## Output Tables

All outputs use `public.l2m_*` prefix:
- `l2m_active` - Daily active users
- `l2m_recharge` - Payment transactions
- `l2m_cumulative_revenue` - Revenue by cohort
- `l2m_item_gain / l2m_item_spend` - Item flow
- `l2m_money_gain / l2m_money_spend` - Currency flow
- `l2m_server_performance` - Server KPIs

## Key Transformations

1. **Schema Mapping**: Old column names → standard names
2. **Type Casting**: String dates → proper date types
3. **Currency Conversion**: Local currency → USD
4. **VIP Calculation**: Total spend → VIP level tiers
5. **Aggregation**: Transaction → daily summary level

## Related Documentation

- [ETL Engine Guide](../../docs/ETL_ENGINE_GUIDE.md)
- [Game Health Check README](../game_health_check/README.md) - Standard schema
