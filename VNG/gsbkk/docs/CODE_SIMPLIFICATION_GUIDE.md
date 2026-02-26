# Code Simplification Recommendations

## Summary
After refactoring rolling_forecast to use game-specific templates and layouts, several code blocks can now be simplified or removed.

## 1. etl_engine.py - Table Resolution Logic (REMOVABLE)

### Location
File: `src/etl_engine.py`  
Lines: 870-895

### Current Code (Can be removed after all games migrated)
```python
game_id = variables.get('gameId', '')

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

### Why Removable
- Game-specific templates now hardcode table names directly
- No templates use `{{ retentionTable }}`, `{{ chargeTable }}`, `{{ activeTable }}` anymore
- Each game has explicit table paths in its SQL templates

### Impact
- **GNOTH**: Uses `mkt.daily_user_retention` hardcoded in template
- **PWMSEA**: Uses `mkt.daily_user_charge` hardcoded in template  
- **L2M**: Uses `public.user_retention` hardcoded in template

### Migration Check
Before removing, verify NO templates still use these placeholders:
```bash
# Search for placeholder usage
grep -r "{{ retentionTable }}" templates/
grep -r "{{ chargeTable }}" templates/
grep -r "{{ activeTable }}" templates/
```

If no matches found, safe to remove this logic.

---

## 2. game_name.yaml - schema_version Field (REMOVABLE)

### Location
File: `configs/game_configs/game_name.yaml`

### Current Code (Can be removed)
```yaml
gnoth:
  market_type: single
  schema_version: v2  # ← This field no longer needed
  
pwmsea:
  market_type: sea
  schema_version: v2  # ← This field no longer needed
  
l2m:
  market_type: multi
  schema_version: v3  # ← This field no longer needed
```

### Why Removable
- No code reads `schema_version` anymore
- Template selection now based on folder structure, not version suffix
- Each game has dedicated template folder (no versioning needed)

### Simplified Config
```yaml
gnoth:
  market_type: single
  countries:
    - "*":
        country_code: "*"
        game_name: "496 - Gunny Origin - TH"
        rfc_sheet_id: "1wHuH2QSERkACZJlNRXvHnX2HKXk2vDdfVUs75jiX-VU"

pwmsea:
  market_type: sea
  countries:
    - "*":
        country_code: "*"
        game_name: "425 - Perfect World M - SEA"
        rfc_sheet_id: "1NeHH7s2snqcQdpSak4xn7NdwrPXy4_SmOvtxdawADaw"

l2m:
  market_type: multi
  countries:
    - TH:
        country_code: "TH"
        game_name: "JV1 - Lineage 2 - TH"
        rfc_sheet_id: "15ZS3fUtg80QEqcskOWBWDRqvF5ZQmD583yS_PlGmc3w"
    # ... other countries
```

---

## 3. dag_helpers.py - No Changes Needed

### Assessment
File: `dags/utils/dag_helpers.py`

**Current functions are still useful:**
- `create_etl_operator()` - Still needed for BashOperator creation
- `create_pipeline_tasks()` - Still needed for task list generation
- No game-specific logic embedded

**Verdict: Keep as-is**

---

## 4. Old Template Files (OBSOLETE)

### Files that can be archived after migration complete:

#### Rolling Forecast Templates
- `templates/rolling_forecast/active_v2.sql.j2` → Replaced by game-specific versions
- `templates/rolling_forecast/retention_v2.sql.j2` → Replaced by game-specific versions
- `templates/rolling_forecast/charge_v2.sql.j2` → Replaced by game-specific versions
- `templates/rolling_forecast/active.sql.j2` → Replaced (was v1/v3 version)
- `templates/rolling_forecast/retention.sql.j2` → Replaced (was v1/v3 version)
- `templates/rolling_forecast/charge.sql.j2` → Replaced (was v1/v3 version)

#### Rolling Forecast Layouts
- `layouts/rolling_forecast/etl/active.json` → Replaced by game-specific versions
- `layouts/rolling_forecast/etl/retention.json` → Replaced by game-specific versions
- `layouts/rolling_forecast/etl/charge.json` → Replaced by game-specific versions
- `layouts/rolling_forecast/cons/metrics.json` → Replaced by game-specific versions

### Migration Strategy
1. **Don't delete yet** - Keep for reference during migration
2. Move to `archived/` folder:
   ```
   templates/rolling_forecast/archived/
     ├── active.sql.j2
     ├── active_v2.sql.j2
     ├── retention.sql.j2
     ├── retention_v2.sql.j2
     ├── charge.sql.j2
     └── charge_v2.sql.j2
   
   layouts/rolling_forecast/archived/
     ├── etl/
     └── cons/
   ```
3. After all games migrated and tested, can safely delete

---

## 5. consolidated_rolling_forecast_dag.py - Phased Deprecation

### Location
File: `dags/rolling_forecast/consolidated_rolling_forecast_dag.py`

### Current State
```python
SCHEDULES = {
    # 'cft': ('0 3 * * *', datetime(2025, 1, 6)),  # Commented out
    # 'fw2': ('0 7 * * *', datetime(2025, 1, 6)),  # Commented out
    'gnoth': ('0 5 * * *', datetime(2025, 1, 6)),  # ← Can remove (has dedicated DAG now)
    'pwmsea': ('0 10 * * *', datetime(2025, 1, 6)),  # ← Can remove (has dedicated DAG now)
    # 'l2m': ('0 13 * * *', datetime(2025, 1, 6)),  # Already moved
}
```

### Recommended Changes
1. Remove `gnoth` from SCHEDULES (now in `gnoth_rolling_forecast_dag.py`)
2. Remove `pwmsea` from SCHEDULES (now in `pwmsea_rolling_forecast_dag.py`)
3. If SCHEDULES becomes empty, can delete entire file

### Timeline
- **Immediate**: Remove gnoth, pwmsea entries
- **Short term**: Migrate remaining games (cft, fw2, etc.)
- **Long term**: Delete consolidated_rolling_forecast_dag.py entirely

---

## 6. Testing Checklist

Before removing any code, verify:

### Template Placeholder Search
```bash
# No templates should use these placeholders anymore
grep -r "{{ retentionTable }}" templates/rolling_forecast/
grep -r "{{ chargeTable }}" templates/rolling_forecast/
grep -r "{{ activeTable }}" templates/rolling_forecast/
grep -r "{{ user_profileTable }}" templates/rolling_forecast/
```

### Schema Version References
```bash
# No code should read schema_version anymore
grep -r "schema_version" src/
grep -r "schema_version" dags/rolling_forecast/
```

### Game-Specific DAG Testing
1. Test `gnoth_rolling_forecast_dag.py` end-to-end
2. Test `pwmsea_rolling_forecast_dag.py` end-to-end
3. Verify parquet data has expected aggregation (1 row per country)
4. Verify postgres writes succeed with deleteCondition
5. Verify Google Sheet updates succeed

---

## Summary

| Component | Action | Status |
|-----------|--------|--------|
| etl_engine.py table resolution | Remove lines 870-895 | Safe after template verification |
| game_name.yaml schema_version | Remove field | Safe now |
| dag_helpers.py | Keep as-is | No changes needed |
| Old template files | Archive to archived/ | After migration complete |
| consolidated_rolling_forecast_dag.py | Remove game entries | Safe now for gnoth/pwmsea |
| Template placeholder usage | Verify none remain | Before removing etl_engine logic |

**Next Action**: Run grep searches to verify no templates use table placeholders, then remove etl_engine.py table resolution logic.
