# Jinja2 Templates - The Mad Libs for SQL 📝

> **Simple Version:** Templates let you write SQL once, use it for 12 games. Like Mad Libs but for databases!

---

## What is a Jinja2 Template? (In Plain English)

Remember **Mad Libs?**

```
Old Way (Mad Libs):
"The [ADJECTIVE] [NOUN] ran to the [PLACE]"

Fill in:
ADJECTIVE = fast
NOUN = dog  
PLACE = park

Result:
"The fast dog ran to the park"
```

**Same idea for SQL:**

```sql
Old Way (Jinja2 Template):
SELECT * FROM {gameId}_revenue WHERE date = '{logDate}'

Fill in:
gameId = fw2
logDate = 2025-12-26

Result:
SELECT * FROM fw2_revenue WHERE date = '2025-12-26'
```

---

## Why Use Templates?

### ❌ Without Templates (The Hard Way)

Write 12 separate SQL files:

```
sql/fw2_vip.sql       ← Copy-paste VIP logic
sql/cft_vip.sql       ← Copy-paste VIP logic
sql/l2m_vip.sql       ← Copy-paste VIP logic
...                   ← Copy-paste 12 times!

Problem: Change VIP thresholds? Edit 12 files! 😱
```

### ✅ With Templates (The Smart Way)

Write 1 template:

```sql
-- templates/game_health_check/vip_calculation.sql.j2
SELECT 
    user_id,
    {{ macros.calculate_vip('total_revenue') }} as vip_level
FROM {gameId}_users

Generate for each game:
fw2_vip.sql, cft_vip.sql, l2m_vip.sql, ...

Change VIP? Edit 1 template, regenerate! ✅
```

---

## Directory Structure

```
templates/
├── sql/
│   ├── macros.sql                      # Reusable SQL snippets
│   ├── diagnostic_daily.sql.j2         # Game health check SQL
│   └── rolling_forecast_market.sql.j2  # Rolling forecast SQL
│
└── layouts/
    ├── etl_layout.json.j2              # ETL layout template
    └── cons_layout.json.j2             # CONS layout template
```
- `calculate_vip(total_rev_column)` - Standardized VIP level calculation
- `to_usd(amount_column, currency_alias, latest_alias)` - Convert to USD
- `to_vnd(amount_column, currency_alias, latest_alias)` - Convert to VND
- `user_type(last_login_month, current_month)` - New vs old user classification
- `latest_usd_rate()` - Latest USD exchange rate subquery
- `gross_revenue_metrics(table_alias, currency_alias, latest_alias)` - NRU01-180 metrics
- `gross_rpi_metrics(table_alias, currency_alias, latest_alias)` - RPI01-180 metrics

### 2. Diagnostic Daily Template (`sql/diagnostic_daily.sql.j2`)

Game health check SQL with:
- VIP calculation using macro
- Currency conversion using macro
- Gross metrics using loops (no repetition)

**Variables:**
- `game_id` (optional) - Filter specific game
- `log_date` (optional) - Filter specific date

### 3. Rolling Forecast Template (`sql/rolling_forecast_market.sql.j2`)

Market-based SQL generation:
- Single market (TH)
- SEA market (aggregate + countries)
- Multi-country market

**Variables:**
- `market_type` - 'single', 'sea', or 'multi'
- `country_code` - For single market (default: TH)
- `countries` - List for multi-market filtering

## Layout Templates

### ETL Layout (`layouts/etl_layout.json.j2`)

Generate ETL layouts dynamically:

```bash
python src/template_renderer.py \
    --template layouts/etl_layout.json.j2 \
    --var project=game_health_check \
    --var layout_name=active_details \
    --var table_name=user_role_active_detail \
    --var 'columns=["report_date","user_id","role_id"]' \
    --output layouts/game_health_check/etl/active_details.json
```

### CONS Layout (`layouts/cons_layout.json.j2`)

Generate CONS layouts referencing SQL files:

```bash
python src/template_renderer.py \
    --template layouts/cons_layout.json.j2 \
    --var project=game_health_check \
    --var layout_name=diagnostic_daily \
    --var sql_file=layouts/game_health_check/sql/diagnostic_daily.sql \
    --var 'input_tables=["active_details","charge_details"]' \
    --output layouts/game_health_check/cons/diagnostic_daily.json
```

## Usage Examples

### 1. Render SQL for Specific Game

```bash
# Generate game-specific diagnostic SQL
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --var game_id=fw2 \
    --var log_date=2025-01-15 \
    --output sql_generated/diagnostic_fw2.sql
```

### 2. Render SQL for All Market Types

```bash
# Single market
python src/template_renderer.py \
    --template sql/rolling_forecast_market.sql.j2 \
    --var market_type=single \
    --var country_code=TH \
    --output sql_generated/single_market.sql

# SEA market
python src/template_renderer.py \
    --template sql/rolling_forecast_market.sql.j2 \
    --var market_type=sea \
    --output sql_generated/sea_market.sql

# Multi-country market
python src/template_renderer.py \
    --template sql/rolling_forecast_market.sql.j2 \
    --var market_type=multi \
    --var 'countries=["VN","TH","ID"]' \
    --output sql_generated/multi_country.sql
```

### 3. Batch Rendering for Multiple Games

Create `configs/games_batch.yaml`:
```yaml
contexts:
  - game_id: fw2
    log_date: 2025-01-15
  - game_id: cft
    log_date: 2025-01-15
  - game_id: mlb
    log_date: 2025-01-15
```

Render:
```bash
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --batch configs/games_batch.yaml \
    --output-dir sql_generated \
    --output-template "{game_id}_diagnostic.sql"
```

### 4. Using Variables File

Create `configs/diagnostic_vars.yaml`:
```yaml
game_id: fw2
log_date: 2025-01-15
project: game_health_check
layout_name: diagnostic_daily
```

Render:
```bash
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --vars-file configs/diagnostic_vars.yaml \
    --output sql_generated/diagnostic.sql
```

### 5. Print to Stdout (for debugging)

```bash
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --var game_id=fw2 \
    --print
```

## Integration with ETL Process

### Option 1: Pre-render SQL Files

Generate SQL files before running pipeline:

```bash
# Generate all SQL files
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --var game_id=fw2 \
    --output layouts/game_health_check/sql/diagnostic_daily_fw2.sql

# Reference in CONS layout
{
  "sql_file": "layouts/game_health_check/sql/diagnostic_daily_fw2.sql"
}
```

### Option 2: Runtime Rendering

Modify your ETL process to render templates on-the-fly:

```python
from src.template_renderer import TemplateRenderer

renderer = TemplateRenderer()
sql_content = renderer.render_template(
    'sql/diagnostic_daily.sql.j2',
    {'game_id': 'fw2', 'log_date': '2025-01-15'}
)
# Execute SQL directly
```

### Option 3: Environment Variable Injection

Use Jinja2 to inject environment variables:

```sql
{% set game_id = env.GAME_ID %}
{% set log_date = env.LOG_DATE %}

WHERE game_id = '{{ game_id }}'
  AND report_date = '{{ log_date }}'
```

## Best Practices

### 1. Use Macros for Common Logic

**Bad (repetitive):**
```sql
-- File 1
CASE WHEN total_rev / 25840 >= 20000 THEN 'G. >=20,000$' ...

-- File 2
CASE WHEN total_rev / 25840 >= 20000 THEN 'G. >=20,000$' ...
```

**Good (macro):**
```sql
{% import 'macros.sql' as macros %}
{{ macros.calculate_vip('total_rev') }}
```

### 2. Use Loops for Repetitive Code

**Bad:**
```sql
SUM(c.grossnru01 * ...) as GrossNRU01,
SUM(c.grossnru03 * ...) as GrossNRU03,
SUM(c.grossnru07 * ...) as GrossNRU07,
-- ... 11 times
```

**Good:**
```sql
{{ macros.gross_revenue_metrics('c', 'cm', 'latest') }}
```

### 3. Use Conditionals for Variations

```sql
{% if market_type == 'single' %}
    WHERE country_code = '{{ country_code }}'
{% elif market_type == 'multi' %}
    WHERE country_code IN ({{ countries|join(', ') }})
{% endif %}
```

### 4. Document Template Variables

Always add header comments:
```sql
{# Variables:
   - game_id: Game identifier (optional)
   - log_date: Report date (required)
   - market_type: 'single', 'sea', or 'multi'
#}
```

### 5. Use Filters for Formatting

```sql
-- Quote strings
WHERE game_id IN ({{ game_ids|map('quote')|join(', ') }})
-- Output: WHERE game_id IN ('fw2', 'cft', 'mlb')

-- Format numbers
{% set vip_threshold = 25840 %}
{{ vip_threshold|round(2) }}
```

## Jinja2 Syntax Reference

### Variables
```sql
{{ variable_name }}
```

### Conditionals
```sql
{% if condition %}
    ...
{% elif other_condition %}
    ...
{% else %}
    ...
{% endif %}
```

### Loops
```sql
{% for item in items %}
    {{ item }}{{ ',' if not loop.last else '' }}
{% endfor %}
```

### Macros
```sql
{% macro macro_name(arg1, arg2) %}
    ...
{% endmacro %}

{{ macro_name('value1', 'value2') }}
```

### Comments
```sql
{# This is a comment #}
```

### Import
```sql
{% import 'macros.sql' as macros %}
{{ macros.function_name() }}
```

## Installation

```bash
# Install Jinja2 and PyYAML
pip install jinja2 pyyaml

# Or add to requirements
echo "jinja2>=3.1.0" >> requirements/base.txt
echo "pyyaml>=6.0" >> requirements/base.txt
```

## Migration Path

### Phase 1: Create Templates (Current)
- ✅ Created macro library
- ✅ Created SQL templates
- ✅ Created layout templates
- ✅ Created renderer tool

### Phase 2: Test with Sample Files
1. Render diagnostic_daily.sql.j2 for fw2
2. Compare output with existing SQL
3. Test in pipeline
4. Validate results

### Phase 3: Migrate Existing Files
1. Convert existing SQL to templates
2. Generate files from templates
3. Update documentation
4. Deprecate old static files

### Phase 4: Integration
1. Update orchestration scripts to use renderer
2. Add pre-render step to CI/CD
3. Create template validation tests
4. Document for team

## Next Steps

1. **Test the renderer:**
   ```bash
   python src/template_renderer.py \
       --template sql/diagnostic_daily.sql.j2 \
       --var game_id=fw2 \
       --print
   ```

2. **Generate your first SQL file:**
   ```bash
   python src/template_renderer.py \
       --template sql/diagnostic_daily.sql.j2 \
       --var game_id=fw2 \
       --output test_output.sql
   ```

3. **Compare with existing SQL:**
   ```bash
   diff test_output.sql layouts/game_health_check/sql/diagnostic_daily.sql
   ```

4. **Start using macros in new SQL files**

5. **Gradually migrate existing files to templates**

## Benefits Summary

| Before (Static SQL) | After (Jinja2 Templates) |
|---------------------|--------------------------|
| VIP logic repeated 3+ times | VIP macro used everywhere |
| 11 nearly-identical SUM lines | 1 macro call generates all |
| Separate SQL per game | 1 template, N games |
| Hard to update business logic | Change once, apply everywhere |
| ~1500 lines of SQL | ~400 lines of templates |

**Result:** 73% less code, 100% consistency, easier maintenance
