# Rolling Forecast DAG Refactoring - Simplification Summary

## Overview
Refactored rolling_forecast from a factory pattern with template versioning to game-specific DAGs with dedicated templates and layouts.

## Changes Made

### 1. DAG Structure
**Before:**
- Single consolidated factory DAG: `consolidated_rolling_forecast_dag.py`
- Used `create_rolling_forecast_dag()` factory function
- Loaded game configs from `game_name.yaml` with `schema_version` field
- Created DAGs dynamically in a loop

**After:**
- 3 separate, standalone DAGs:
  - `gnoth_rolling_forecast_dag.py` (single market)
  - `pwmsea_rolling_forecast_dag.py` (SEA aggregate market)
  - `l2m_rolling_forecast_dag.py` (multi-country market, already existed)
- Each DAG hardcodes its own configuration
- No factory pattern, no dynamic generation
- No schema_version lookups

### 2. Layout Files
**Before:**
- Shared layouts in `layouts/rolling_forecast/etl/` and `cons/`
- Used `{gameId}` placeholders in paths
- Single layout served all games

**After:**
- Game-specific layout folders:
  ```
  layouts/rolling_forecast/
    ├── gnoth/
    │   ├── etl/
    │   │   ├── active.json
    │   │   ├── retention.json
    │   │   └── charge.json
    │   └── cons/
    │       └── metrics.json
    ├── pwmsea/
    │   ├── etl/
    │   │   ├── active.json
    │   │   ├── retention.json
    │   │   └── charge.json
    │   └── cons/
    │       └── metrics.json
    └── l2m/
        ├── etl/
        │   ├── active.json
        │   ├── retention.json
        │   └── charge.json
        └── cons/
            └── metrics.json
  ```
- Paths hardcoded per game (e.g., `rolling_forecast/gnoth/active/`)
- Configurations explicit and isolated

### 3. Jinja2 Templates
**Before:**
- Template versioning: `active.sql.j2`, `active_v2.sql.j2`
- Used `{{ retentionTable }}`, `{{ chargeTable }}`, `{{ activeTable }}` placeholders
- Schema resolution logic in `etl_engine.py` based on game_id

**After:**
- Game-specific template folders:
  ```
  templates/rolling_forecast/
    ├── gnoth/
    │   ├── active.sql.j2
    │   ├── retention.sql.j2
    │   └── charge.sql.j2
    ├── pwmsea/
    │   ├── active.sql.j2
    │   ├── retention.sql.j2
    │   └── charge.sql.j2
    └── l2m/
        ├── active.sql.j2
        ├── retention.sql.j2
        └── charge.sql.j2
  ```
- **No more template versioning** (_v2, _v3 suffixes eliminated)
- **Tables hardcoded** in each template:
  - GNOTH/PWMSEA: `mkt.daily_user_retention`, `mkt.daily_user_charge`
  - L2M: `public.user_retention`, `public.ghc_diagnostic_daily_l2m`
- **Game IDs hardcoded** in WHERE clauses:
  - GNOTH: `WHERE game_id = 496`
  - PWMSEA: `WHERE game_id = 425`
  - L2M: `WHERE game_id = 'l2m'`

### 4. Code Simplifications Available

#### etl_engine.py (Lines 870-895)
**Can be removed:**
```python
# Common tables group 1: cft, fw2, mlb, mts, slth, l2m, skf
common_tables_1 = ['cft', 'fw2', 'mlb', 'mts', 'slth', 'l2m', 'skf']
# Common tables group 2: pwmsea, td, gnoth, omgthai, cloudsongsea
common_tables_2 = ['pwmsea', 'td', 'gnoth', 'omgthai', 'cloudsongsea', 'cloudsongthai']

if game_id in common_tables_1:
    variables['activeTable'] = 'public.mkt_user_active'
    variables['chargeTable'] = 'public.mkt_user_charge'
    variables['retentionTable'] = 'public.mkt_user_retention'
    variables['user_profileTable'] = 'public.mkt_user_profile'
elif game_id in common_tables_2:
    variables['activeTable'] = 'mkt.daily_user_active'
    variables['chargeTable'] = 'mkt.daily_user_charge'
    variables['retentionTable'] = 'mkt.daily_user_retention'
    variables['user_profileTable'] = 'ops.user_profile'
else:
    # Default to common_tables_1
    variables['activeTable'] = 'public.mkt_user_active'
    variables['chargeTable'] = 'public.mkt_user_charge'
    variables['retentionTable'] = 'public.mkt_user_retention'
    variables['user_profileTable'] = 'public.mkt_user_profile'
```

**Reason:** Table names now hardcoded in game-specific templates. No need for dynamic resolution.

#### game_name.yaml
**Can be removed:**
```yaml
schema_version: v2  # No longer needed
schema_version: v3  # No longer needed
```

**Reason:** No template versioning anymore. Each game has its own dedicated templates.

### 5. Benefits

#### Clarity
- Each DAG file is self-contained and readable
- No need to trace through factory functions
- Template paths explicit in DAG code

#### Debugging
- Errors clearly tied to specific game
- No confusion about which template version is used
- No schema resolution ambiguity

#### Maintenance
- Add new game = create new folder + DAG file
- No risk of breaking other games when editing one game's templates
- Clear ownership and isolation

#### Performance
- No dynamic game_id substitution in paths
- No runtime schema resolution logic
- Templates pre-resolved to specific tables

### 6. Migration Path

For each game still using consolidated DAG:
1. Create `layouts/rolling_forecast/{game}/` folder structure
2. Create `templates/rolling_forecast/{game}/` folder with SQL templates
3. Hardcode table names (remove `{{ retentionTable }}` placeholders)
4. Hardcode game_id filters (remove `{{ gameId }}` in WHERE clauses)
5. Create dedicated `{game}_rolling_forecast_dag.py` file
6. Remove from `SCHEDULES` dict in `consolidated_rolling_forecast_dag.py`

### 7. Next Steps (Optional)

Apply same pattern to:
- **game_health_check** pipeline (same factory pattern complexity)
- **re_standardization** pipeline if applicable
- Other pipelines using game_name.yaml schema_version field

This refactoring eliminates:
- Factory pattern complexity
- Template versioning system
- Schema resolution logic
- game_path.yaml dependencies
- Dynamic game_id path substitution

Result: **Simpler, more maintainable, easier to debug.**
