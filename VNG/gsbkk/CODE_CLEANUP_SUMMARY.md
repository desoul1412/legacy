# Source Code Cleanup Summary

## Overview
After refactoring `rolling_forecast` and `game_health_check` pipelines to use game-specific structures, several pieces of code have been removed or deprecated.

## Changes Made

### 1. etl_engine.py - Removed Table Name Mappings
**File**: `src/etl_engine.py`  
**Lines**: 868-892 (now replaced with comment)

**What was removed**:
```python
# Old code that mapped gameId to table names
if game_id in common_tables_1:
    variables['activeTable'] = 'public.mkt_user_active'
    variables['chargeTable'] = 'public.mkt_user_charge'
    variables['retentionTable'] = 'public.mkt_user_retention'
    variables['user_profileTable'] = 'public.mkt_user_profile'
elif game_id in common_tables_2:
    variables['activeTable'] = 'mkt.daily_user_active'
    # ... etc
```

**Why it was removed**:
- Templates no longer use `{{activeTable}}`, `{{chargeTable}}`, `{{retentionTable}}` placeholders
- Each game now has dedicated templates with hardcoded table names
- Searched entire codebase: 0 matches for these placeholders in templates

**What remains**:
- `resolve_table_path()` function (lines 26-58) - Still used by `re_standardization` pipeline
- tableKey resolution logic (lines 490-498) - Still used by `re_standardization`

### 2. l2m_game_health_check_dag.py - Removed Schema Version Reference
**File**: `dags/game_health_check/l2m_game_health_check_dag.py`  
**Line**: 35

**Changed from**:
```python
description=f'L2M game health monitoring - {SCHEMA_VERSION} with Trino sources',
```

**Changed to**:
```python
description='L2M game health monitoring with Trino sources',
```

**Why**:
- SCHEMA_VERSION variable was not defined in the file (would cause NameError)
- Schema versioning is no longer needed since each game has dedicated templates

## What Can Still Be Removed (Future Cleanup)

### game_name.yaml - schema_version Fields
**File**: `configs/game_configs/game_name.yaml`  
**Lines**: 4, 13, 22, 60

```yaml
pwmsea:
  schema_version: v2  # <-- No longer used
  
gnoth:
  schema_version: v2  # <-- No longer used
  
l2m:
  schema_version: v3  # <-- No longer used
  
lw:
  schema_version: v3  # <-- No longer used
```

**Status**: Can be removed if no other pipelines use schema_version  
**Action**: Verify with `grep -r "schema_version" dags/` first

## What CANNOT Be Removed

### resolve_table_path() Function
**File**: `src/etl_engine.py`  
**Lines**: 26-58

**Status**: Still in use by `re_standardization` pipeline  
**Evidence**:
- `layouts/re-standardization/l2m/cons/l2m_server_performance.json` uses `tableKey` (line 22)
- `dags/re_standardization/l2m_re_standardization_dag.py` is active

## Verification

### Templates - No More Placeholders
```bash
# Search for old table placeholders in templates
grep -r "{{.*Table}}" templates/
# Result: 0 matches ✓
```

### Layouts - Only 1 Layout Uses tableKey
```bash
# Search for tableKey in layouts
grep -r "tableKey" layouts/**/*.json
# Result: 1 match in re-standardization/l2m/cons/l2m_server_performance.json ✓
```

### DAGs - No More Template Versioning
```bash
# Search for _v2/_v3 template references
grep -r "_v2\|_v3\.sql\.j2" dags/
# Result: 0 matches ✓
```

## Impact Assessment

### Pipelines Cleaned Up ✅
- `rolling_forecast`: gnoth, pwmsea, l2m
- `game_health_check`: gnoth, pwmsea, l2m

### Pipelines Still Using Old Pattern
- `re_standardization`: l2m (uses tableKey resolution)

### Code Size Reduction
- **etl_engine.py**: Removed ~27 lines of table mapping logic
- **Templates**: Removed 14 versioned template files (_v2, _v3)
- **Layouts**: Removed 6 shared layout folders

## Next Steps (Optional)

1. **If re_standardization can be refactored**:
   - Remove `resolve_table_path()` function (lines 26-58)
   - Remove tableKey resolution logic (lines 490-498)
   - Remove `schema_version` from `game_name.yaml`

2. **Test Changes**:
   ```bash
   # Test each DAG individually
   python dags/rolling_forecast/gnoth_rolling_forecast_dag.py
   python dags/rolling_forecast/pwmsea_rolling_forecast_dag.py
   python dags/rolling_forecast/l2m_rolling_forecast_dag.py
   python dags/game_health_check/gnoth_game_health_check_dag.py
   python dags/game_health_check/pwmsea_game_health_check_dag.py
   python dags/game_health_check/l2m_game_health_check_dag.py
   ```

3. **Monitor Production**:
   - Verify no errors related to missing template variables
   - Check that data writes are successful
   - Confirm no duplicate data issues

## Documentation References
- [ROLLING_FORECAST_REFACTORING.md](ROLLING_FORECAST_REFACTORING.md) - Detailed refactoring plan
- [CODE_SIMPLIFICATION_GUIDE.md](CODE_SIMPLIFICATION_GUIDE.md) - Simplification guide
