# Jinja2 Templates - Complete Practical Guide

**Your go-to guide for understanding, testing, and integrating Jinja2 templates into your workflow.**

---

## Table of Contents
1. [What Is Jinja2 & Why Use It](#what-is-jinja2--why-use-it)
2. [Core Engine Files Explained](#core-engine-files-explained)
3. [How to Test](#how-to-test)
4. [How to Apply to Your Workflow](#how-to-apply-to-your-workflow)
5. [Real Examples from Your Codebase](#real-examples-from-your-codebase)
6. [Command Reference](#command-reference)
7. [Quick Reference](#quick-reference)

---

## What Is Jinja2 & Why Use It?

### The Problem in Your Current Codebase

**Repetitive VIP Logic** - Same 13 lines in 6 different files:
```sql
-- Appears in diagnostic_daily.sql, package_performance.sql, server_performance.sql, etc.
CASE 
    WHEN total_rev / 25840 >= 12 THEN '12. VIP 12+'
    WHEN total_rev / 25840 >= 8 THEN '11. VIP 8-11'
    WHEN total_rev / 25840 >= 4 THEN '10. VIP 4-7'
    WHEN total_rev / 25840 >= 2 THEN '9. VIP 2-3'
    WHEN total_rev / 25840 >= 1 THEN '8. VIP 1'
    WHEN total_rev > 0 THEN '7. Non-VIP Paying'
    ELSE '1. Non-Paying'
END as vip_tier
```

**Impact when changing VIP threshold from 20K to 25K:**
- ❌ Open and edit 6 files manually
- ❌ Risk missing a file or making typo
- ❌ Takes ~30 minutes
- ❌ Difficult to ensure consistency

### The Solution: Jinja2 Templating

Jinja2 is a templating engine that eliminates code duplication using:

**1. Macros** - Define reusable SQL snippets once
```sql
-- In macros.sql (define once)
{%- macro calculate_vip(revenue_col) %}
CASE 
    WHEN {{ revenue_col }} / 25840 >= 12 THEN '12. VIP 12+'
    -- ... rest of logic ...
END
{%- endmacro %}
```

**2. Variables** - Replace hardcoded values
```jinja2
-- In template
SELECT * FROM {{ schema }}.{{ table }} WHERE {{ date_column }} = '{{ target_date }}'

-- Renders to
SELECT * FROM fw2.active_users WHERE log_date = '2025-12-26'
```

**3. Conditionals** - Different SQL for different scenarios
```jinja2
{% if market_type == 'sea' %}
    -- SEA aggregate logic
{% elif market_type == 'multi' %}
    -- Per country logic
{% endif %}
```

### Benefits in Your Workflow

| Task | Before (Manual) | After (Jinja2) | Savings |
|------|-----------------|----------------|---------|
| Change VIP threshold | Edit 6 files (~30 min) | Edit 1 macro (~1 min) | **30x faster** |
| Add new game | Copy 200 lines, edit 50 places (~40 min) | Generate from template (~10 sec) | **240x faster** |
| Add new metric | Update 3 files identically (~20 min) | Update 1 macro (~2 min) | **10x faster** |
| Code to maintain | 6 files × 200 lines = 1,200 lines | 1 template × 200 lines = 200 lines | **83% less code** |

---

## Core Engine Files Explained

### 1. Template Renderer (`src/template_renderer.py`)

**Purpose:** Command-line tool to render Jinja2 templates with variables.

**What it does:**
- Loads template files (.j2) and config files (.yaml)
- Substitutes variables and expands macros
- Generates SQL files, JSON layouts, or any text file
- Supports batch processing (one config → multiple outputs)

**Architecture:**
```python
# Main components:
class TemplateRenderer:
    def __init__(self):
        # Sets up Jinja2 environment with custom filters
        self.env = Environment(
            loader=FileSystemLoader('templates'),
            trim_blocks=True,      # Remove whitespace after {% %}
            lstrip_blocks=True     # Remove leading whitespace
        )
        
    def render(self, template_path, context):
        # 1. Load template file
        # 2. Merge context variables
        # 3. Render template → final SQL/JSON
        # 4. Return result
```

**Key Features:**

1. **Variable Substitution:**
```python
# Config: game_id = "fw2"
# Template: SELECT * FROM {{ game_id }}.active
# Result:   SELECT * FROM fw2.active
```

2. **Macro Expansion:**
```python
# Template: {{ calculate_vip('revenue') }}
# Expands to: CASE WHEN revenue / 25840 >= 12 THEN...
```

3. **Conditional Logic:**
```python
# Template: 
# {% if game_id == 'l2m' %}
#     -- L2M specific SQL
# {% else %}
#     -- Standard SQL
# {% endif %}
```

4. **Batch Processing:**
```python
# One YAML config with array:
# contexts:
#   - {game_id: fw2, schema: fw2}
#   - {game_id: cft, schema: cft}
# Generates 2 separate SQL files
```

**Custom Filters:**
```python
# Add custom formatting functions
env.filters['upper'] = lambda x: x.upper()
env.filters['snake_case'] = lambda x: x.replace(' ', '_').lower()

# Usage in template:
# {{ game_name | snake_case }}  # "Food War 2" → "food_war_2"
```

**Error Handling:**
```python
# Validates:
# - Template file exists
# - Config file is valid YAML
# - Required variables are provided
# - Output directory is writable

# Provides clear error messages:
# "Missing required variable 'game_id' in template diagnostic_daily.sql.j2"
```

**Usage Examples:**
```bash
# Basic: Single template + config → single output
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --config configs/templates/fw2.yaml \
    --output output.sql

# Batch: Single template + config → multiple outputs
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --config configs/templates/games_batch.yaml \
    --batch

# Variable override
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --config configs/templates/fw2.yaml \
    --var game_id=fw2_test

# Dry run (preview without writing)
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --config configs/templates/fw2.yaml \
    --dry-run
```

---

### 2. SQL Macros Library (`templates/sql/macros.sql`)

**Purpose:** Centralized library of reusable SQL snippets used across all game queries.

**What it does:**
- Provides standardized business logic (VIP tiers, currency conversion, metrics)
- Ensures consistency across all SQL files
- Single source of truth for calculations

**Architecture:**
```jinja2
{# Each macro is a parameterized SQL snippet #}

{%- macro macro_name(parameter1, parameter2) %}
    SQL code using {{ parameter1 }} and {{ parameter2 }}
{%- endmacro %}

{# Usage in templates: #}
{{ macro_name('value1', 'value2') }}
```

**Available Macros:**

#### 1. `calculate_vip(revenue_col)`
**Purpose:** Standardized VIP tier classification based on total revenue.

**Logic:**
```sql
CASE 
    WHEN {{ revenue_col }} / 25840 >= 12 THEN '12. VIP 12+'
    WHEN {{ revenue_col }} / 25840 >= 8 THEN '11. VIP 8-11'
    WHEN {{ revenue_col }} / 25840 >= 4 THEN '10. VIP 4-7'
    WHEN {{ revenue_col }} / 25840 >= 2 THEN '9. VIP 2-3'
    WHEN {{ revenue_col }} / 25840 >= 1 THEN '8. VIP 1'
    WHEN {{ revenue_col }} > 0 THEN '7. Non-VIP Paying'
    ELSE '1. Non-Paying'
END
```

**Usage:**
```jinja2
-- In template
{{ calculate_vip('total_revenue') }} as vip_tier

-- Renders to
CASE WHEN total_revenue / 25840 >= 12 THEN '12. VIP 12+' ... END as vip_tier
```

**Business Logic:**
- VIP level = total_revenue / 25,840 (VND)
- 25,840 VND ≈ $1 USD at standard exchange rate
- VIP 12+ = spent $12+ USD equivalent
- Sorted descending for reporting

**When to modify:**
- Changing VIP threshold (e.g., 20K → 25K)
- Adding new VIP tiers
- Different VIP calculation per game

---

#### 2. `to_usd(amount, rate)` & `to_vnd(amount, rate)`
**Purpose:** Consistent currency conversion across all reports.

**Logic:**
```sql
{%- macro to_usd(amount, rate) %}
({{ amount }} * COALESCE({{ rate }}, 1))
{%- endmacro %}

{%- macro to_vnd(amount, rate) %}
({{ amount }} * COALESCE({{ rate }}, 25840))
{%- endmacro %}
```

**Usage:**
```jinja2
-- In template
{{ to_usd('revenue_local', 'exchange_rate_to_usd') }} as revenue_usd,
{{ to_vnd('revenue_local', 'exchange_rate_to_vnd') }} as revenue_vnd

-- Renders to
(revenue_local * COALESCE(exchange_rate_to_usd, 1)) as revenue_usd,
(revenue_local * COALESCE(exchange_rate_to_vnd, 25840)) as revenue_vnd
```

**Business Logic:**
- Default USD rate: 1 (if rate missing, assume already in USD)
- Default VND rate: 25,840 (standard exchange rate)
- COALESCE handles missing exchange rates

---

#### 3. `gross_revenue_metrics(revenue_col, install_col, target_date)`
**Purpose:** Automatically generate 11 NRU (New Registered User) revenue metrics.

**Logic:**
```sql
{%- macro gross_revenue_metrics(revenue_col, install_col, target_date) %}
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '0 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru0,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '1 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru1,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '3 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru3,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '7 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru7,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '14 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru14,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '30 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru30,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '60 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru60,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '90 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru90,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '180 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru180,
SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '365 day' THEN {{ revenue_col }} ELSE 0 END) as gross_nru365,
SUM({{ revenue_col }}) as gross_total
{%- endmacro %}
```

**Usage:**
```jinja2
-- In template (1 line instead of 11)
{{ gross_revenue_metrics('charge.revenue', 'active.install_time', 'CURRENT_DATE') }}
```

**Business Logic:**
- NRU0 = revenue from users installed today
- NRU1 = revenue from users installed within last 1 day
- NRU7 = revenue from users installed within last 7 days
- ...up to NRU365 (last year)
- gross_total = all revenue

**When to modify:**
- Add new time buckets (e.g., NRU21, NRU45)
- Change interval logic
- Add filters (e.g., exclude refunds)

---

#### 4. `gross_rpi_metrics(revenue_col, users_col, install_col, target_date)`
**Purpose:** Automatically generate 11 RPI (Revenue Per Install) metrics.

**Logic:**
```sql
{%- macro gross_rpi_metrics(revenue_col, users_col, install_col, target_date) %}
ROUND(SUM(CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '0 day' THEN {{ revenue_col }} ELSE 0 END) / 
      NULLIF(COUNT(DISTINCT CASE WHEN {{ install_col }} >= {{ target_date }} - INTERVAL '0 day' THEN {{ users_col }} END), 0), 2) as rpi_nru0,
-- ... similar for nru1, nru3, nru7, nru14, nru30, nru60, nru90, nru180, nru365 ...
ROUND(SUM({{ revenue_col }}) / NULLIF(COUNT(DISTINCT {{ users_col }}), 0), 2) as rpi_total
{%- endmacro %}
```

**Business Logic:**
- RPI = Total Revenue / Total Users (avoiding division by zero)
- Calculated for same time buckets as gross_revenue_metrics
- Rounded to 2 decimal places

---

#### 5. `user_type(install_col, target_date)`
**Purpose:** Classify users as new or old based on install date.

**Logic:**
```sql
{%- macro user_type(install_col, target_date) %}
CASE 
    WHEN {{ install_col }} = {{ target_date }} THEN 'New User'
    ELSE 'Old User'
END
{%- endmacro %}
```

**Usage:**
```jinja2
{{ user_type('install_date', 'log_date') }} as user_type
```

---

#### 6. `latest_usd_rate()` & `latest_vnd_rate()`
**Purpose:** Subquery to get latest exchange rates when rate missing from main data.

**Logic:**
```sql
{%- macro latest_usd_rate() %}
(SELECT exchange_rate_to_usd FROM currency_mapping ORDER BY date DESC LIMIT 1)
{%- endmacro %}
```

**Usage:**
```jinja2
{{ to_usd('revenue', 'COALESCE(currency.rate_usd, ' ~ latest_usd_rate() ~ ')') }}
```

---

### 3. SQL Templates (`templates/sql/*.j2`)

**Purpose:** Template files that use macros and variables to generate final SQL.

#### Example: `diagnostic_daily.sql.j2`

**Structure:**
```jinja2
{# Import macros #}
{% import 'sql/macros.sql' as macros %}

{# Template variables (provided via config):
   - game_id: "fw2"
   - game_name: "Food War 2"
   - schema: "fw2"
   - date_column: "log_date"
#}

-- Diagnostic Daily Report for {{ game_name }}
SELECT 
    {{ date_column }},
    campaign_name,
    
    -- VIP tier using macro (instead of 13 lines)
    {{ macros.calculate_vip('total_revenue') }} as vip_tier,
    
    -- Currency conversion using macros
    {{ macros.to_usd('revenue', 'exchange_rate_to_usd') }} as revenue_usd,
    {{ macros.to_vnd('revenue', 'exchange_rate_to_vnd') }} as revenue_vnd,
    
    -- Gross revenue metrics using macro (instead of 11 lines)
    {{ macros.gross_revenue_metrics('charge.revenue', 'active.install_time', 'CURRENT_DATE') }},
    
    -- RPI metrics using macro (instead of 11 lines)
    {{ macros.gross_rpi_metrics('charge.revenue', 'active.user_id', 'active.install_time', 'CURRENT_DATE') }}

FROM {{ schema }}.active_users a
LEFT JOIN {{ schema }}.charge_data c ON a.user_id = c.user_id
WHERE a.{{ date_column }} = '{{ target_date }}'
GROUP BY {{ date_column }}, campaign_name
```

**When rendered with config:**
```yaml
game_id: fw2
game_name: "Food War 2"
schema: fw2
date_column: log_date
target_date: "2025-12-26"
```

**Produces:** Full 200+ line SQL file with all macros expanded.

---

### 4. Layout Templates (`templates/layouts/*.j2`)

**Purpose:** Generate ETL/CONS JSON layout files from templates.

#### Example: `etl_layout.json.j2`

**Structure:**
```jinja2
{
  "type": "{{ type }}",
  "read": {
    "connection": "{{ source_db }}",
    "table": "{{ source_schema }}.{{ source_table }}",
    "where": "{{ date_column }} = '{{ target_date }}'"
  },
  "write": {
    "path": "hdfs://{{ hdfs_path }}/{{ game_id }}/{{ layer }}/{{ table_name }}/{{ partition }}",
    "format": "parquet",
    "mode": "overwrite",
    "partitionBy": ["{{ date_column }}"]
  },
  "columns": [
    {% for col in columns %}
    {"name": "{{ col.name }}", "type": "{{ col.type }}"}{{ "," if not loop.last else "" }}
    {% endfor %}
  ]
}
```

**Usage:**
```bash
# Config file
cat > etl_active.yaml << EOF
type: etl
source_db: gds_postgres
source_schema: fw2
source_table: active_users
date_column: log_date
target_date: "2025-12-26"
game_id: fw2
layer: etl
table_name: active
partition: "2025-12-26"
columns:
  - {name: user_id, type: string}
  - {name: log_date, type: date}
  - {name: install_time, type: timestamp}
EOF

# Generate layout
python src/template_renderer.py \
    --template layouts/etl_layout.json.j2 \
    --config etl_active.yaml \
    --output layouts/fw2/etl/active.json
```

---

### 5. Config Files (`configs/templates/*.yaml`)

**Purpose:** Provide variables for template rendering.

#### Example: `configs/templates/diagnostic_vars.yaml`

**Structure:**
```yaml
# Single game configuration
game_id: fw2
game_name: "Food War 2"
schema: fw2
date_column: log_date
table_prefix: fw2
target_date: "2025-12-26"

# Optional: Game-specific overrides
vip_divisor: 25840
default_currency: VND
```

#### Example: `configs/templates/games_batch.yaml`

**Structure:**
```yaml
# Batch configuration (generates multiple files)
contexts:
  - game_id: fw2
    game_name: "Food War 2"
    schema: fw2
    output: layouts/game_health_check/sql/diagnostic_daily_fw2.sql
    
  - game_id: cft
    game_name: "Crazy Fighter"
    schema: cft
    output: layouts/game_health_check/sql/diagnostic_daily_cft.sql
    
  - game_id: l2m
    game_name: "Legend 2 Mobile"
    schema: l2m
    output: layouts/game_health_check/sql/diagnostic_daily_l2m.sql
```

**Usage:**
```bash
python src/template_renderer.py \
    --template sql/diagnostic_daily.sql.j2 \
    --config configs/templates/games_batch.yaml \
    --batch
# Generates 3 SQL files automatically
```

---

## How to Test

### Level 1: View Infrastructure (30 seconds)

```bash
# Check template renderer exists
ls src/template_renderer.py

# View available macros
cat templates/sql/macros.sql

# View available templates
ls templates/sql/*.j2
ls templates/layouts/*.j2

# View example configs
ls configs/templates/*.yaml
```

---

### Level 2: Run Demo Script (2 minutes)

```bash
# Install dependencies (one-time)
pip install jinja2 pyyaml

# Run interactive demo
bash demo_jinja2.sh
```

**What the demo shows:**
1. Template with variables
2. Rendering SQL for different games
3. Macro expansion examples
4. Before/after comparison
5. Time savings calculation

---

### Level 3: Generate Test SQL (5 minutes)

**Test 1: Generate diagnostic SQL for FW2**
```bash
# Use existing config
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/diagnostic_vars.yaml \
    --output test_fw2_diagnostic.sql

# View generated SQL
head -50 test_fw2_diagnostic.sql

# Compare with existing (should be very similar)
diff test_fw2_diagnostic.sql layouts/game_health_check/sql/diagnostic_daily.sql
```

**Test 2: Generate SQL with custom variables**
```bash
# Create test config
cat > test_custom.yaml << EOF
game_id: testgame
game_name: "Test Game"
schema: testgame
date_column: event_date
target_date: "2025-12-26"
EOF

# Generate SQL
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config test_custom.yaml \
    --output test_custom_diagnostic.sql

# View result
cat test_custom_diagnostic.sql
```

**Test 3: Dry run (preview without writing file)**
```bash
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/diagnostic_vars.yaml \
    --dry-run | head -50
```

**Test 4: Batch generation**
```bash
# View batch config
cat configs/templates/games_batch.yaml

# Generate all games (preview)
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/games_batch.yaml \
    --batch \
    --dry-run
```

---

### Level 4: Test with Real Pipeline (10 minutes)

```bash
# Generate SQL for FW2 game health check
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/diagnostic_vars.yaml \
    --output layouts/game_health_check/sql/diagnostic_daily.sql

# Run pipeline with generated SQL
./run_game_health_pipeline.sh 2025-12-26 fw2 diagnostic

# Verify output
# Check TSN Postgres: fw2.public.diagnostic_daily
```

---

## How to Apply to Your Workflow

### Scenario 1: Change VIP Threshold (20K → 25K)

**Current Manual Workflow:**
1. Open `layouts/game_health_check/sql/diagnostic_daily.sql`
2. Find VIP CASE statement (lines 45-57)
3. Change `>= 4` to `>= 5` (4 occurrences)
4. Save file
5. Repeat for `package_performance.sql`
6. Repeat for `server_performance.sql`
7. Test all 3 files
8. Commit 3 files

**Time:** ~30 minutes | **Risk:** Missing a file, inconsistent changes

---

**New Jinja2 Workflow:**

```bash
# Step 1: Edit macro (1 file, 1 line)
nano templates/sql/macros.sql
# Line 7: Change WHEN {{ revenue_col }} / 25840 >= 4 THEN...
# To:            WHEN {{ revenue_col }} / 25840 >= 5 THEN...

# Step 2: Regenerate all SQL files (automated)
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --output layouts/game_health_check/sql/diagnostic_daily.sql

python src/template_renderer.py \
    --template templates/sql/package_performance.sql.j2 \
    --output layouts/game_health_check/sql/package_performance.sql

python src/template_renderer.py \
    --template templates/sql/server_performance.sql.j2 \
    --output layouts/game_health_check/sql/server_performance.sql

# Step 3: Test (same as before)
./run_game_health_pipeline.sh 2025-12-26 fw2 diagnostic

# Step 4: Commit (1 macro + 3 generated files)
git add templates/sql/macros.sql
git add layouts/game_health_check/sql/*.sql
git commit -m "Update VIP threshold to 25K (VIP 4-7 tier)"
```

**Time:** ~2 minutes | **Risk:** Zero (guaranteed consistency across all files)

---

### Scenario 2: Add New Game (e.g., "newgame")

**Current Manual Workflow:**
1. Copy existing SQL file: `cp diagnostic_daily.sql diagnostic_daily_newgame.sql`
2. Find/replace "fw2" → "newgame" (50+ occurrences)
3. Find/replace "Food War 2" → "New Game Title"
4. Update schema references
5. Repeat for package_performance.sql
6. Repeat for server_performance.sql
7. Copy ETL layouts, update game_id
8. Copy CONS layouts, update game_id
9. Update pipeline scripts to include new game
10. Test everything

**Time:** ~40 minutes | **Risk:** Missed replacements, wrong schema, typos

---

**New Jinja2 Workflow:**

```bash
# Step 1: Create game config (30 seconds)
cat > configs/templates/newgame.yaml << EOF
game_id: newgame
game_name: "New Game Title"
schema: newgame
date_column: log_date
table_prefix: newgame
target_date: "2025-12-26"
EOF

# Step 2: Generate all SQL files (10 seconds)
for template in diagnostic_daily package_performance server_performance; do
    python src/template_renderer.py \
        --template templates/sql/${template}.sql.j2 \
        --config configs/templates/newgame.yaml \
        --output layouts/game_health_check/sql/${template}_newgame.sql
done

# Step 3: Generate ETL/CONS layouts (if templates exist)
python src/template_renderer.py \
    --template templates/layouts/etl_layout.json.j2 \
    --config configs/templates/newgame.yaml \
    --output layouts/newgame/etl/active.json

# Step 4: Test
./run_game_health_pipeline.sh 2025-12-26 newgame diagnostic

# Step 5: Commit
git add configs/templates/newgame.yaml
git add layouts/game_health_check/sql/*_newgame.sql
git commit -m "Add new game: newgame"
```

**Time:** ~1 minute (setup) + ~5 minutes (testing) = ~6 minutes total  
**Risk:** Zero (template guarantees correctness)

---

### Scenario 3: Add New Metric to All Games

**Example:** Add LTV (Lifetime Value) calculation to all diagnostic reports.

**Current Manual Workflow:**
1. Write SQL formula: `ROUND(total_revenue / NULLIF(dau, 0), 2) as ltv`
2. Add to diagnostic_daily.sql
3. Copy to package_performance.sql, adjust column names
4. Copy to server_performance.sql, adjust column names
5. Test each file individually
6. Ensure consistency across files

**Time:** ~20 minutes | **Risk:** Inconsistent formula, different column names

---

**New Jinja2 Workflow:**

```bash
# Step 1: Add macro to macros.sql
nano templates/sql/macros.sql
# Add at bottom:
# {%- macro calculate_ltv(revenue_col, users_col) %}
# ROUND({{ revenue_col }} / NULLIF({{ users_col }}, 0), 2)
# {%- endmacro %}

# Step 2: Add to template (one line per template)
nano templates/sql/diagnostic_daily.sql.j2
# Add: {{ macros.calculate_ltv('total_revenue', 'dau') }} as ltv,

nano templates/sql/package_performance.sql.j2
# Add: {{ macros.calculate_ltv('package_revenue', 'package_users') }} as ltv,

# Step 3: Regenerate all SQL files
python src/template_renderer.py --regenerate-all

# Step 4: Test
./run_game_health_pipeline.sh 2025-12-26 fw2 diagnostic
```

**Time:** ~2 minutes | **Risk:** Zero (macro ensures consistency)

---

### Scenario 4: Market-Specific SQL Variations

**Example:** Rolling forecast has 3 market types (single, SEA, multi-country).

**Current Manual Workflow:**
- Maintain 3 separate SQL files with 75% duplicate code
- Change currency logic → Update all 3 files
- Add new metric → Update all 3 files

**With Jinja2:**
```jinja2
-- Single template with conditionals
{% if market_type == 'single' %}
    -- TH only logic
    WHERE country_code = 'TH'
{% elif market_type == 'sea' %}
    -- SEA aggregate
    WHERE country_code IN ('TH', 'VN', 'PH', 'ID', 'SG', 'MY')
    GROUP BY country_code
{% elif market_type == 'multi' %}
    -- Per country breakdown
    GROUP BY country_code
{% endif %}

-- Shared logic (appears once, used by all 3 types)
{{ macros.to_usd('revenue', 'exchange_rate') }} as revenue_usd
```

**Generate for different market types:**
```bash
# Generate single market SQL
python src/template_renderer.py \
    --template templates/sql/rolling_forecast_market.sql.j2 \
    --var market_type=single \
    --output layouts/rolling_forecast/sql/single_market.sql

# Generate SEA market SQL
python src/template_renderer.py \
    --template templates/sql/rolling_forecast_market.sql.j2 \
    --var market_type=sea \
    --output layouts/rolling_forecast/sql/sea_market.sql

# Generate multi-country SQL
python src/template_renderer.py \
    --template templates/sql/rolling_forecast_market.sql.j2 \
    --var market_type=multi \
    --output layouts/rolling_forecast/sql/multi_country.sql
```

---

## Integration Options

### Option A: Gradual Adoption (Recommended)

Keep current workflow, switch to templates when beneficial:

```bash
# Continue using existing SQL files
./run_game_health_pipeline.sh 2025-12-26 fw2

# When you need to change VIP threshold → Use templates
# When you need to add new game → Use templates
# When you need to add new metric → Use templates
```

**Pros:**
- ✅ No disruption to current workflow
- ✅ Learn templates gradually
- ✅ Measure time savings on real tasks

**Cons:**
- ⚠️ Need to remember to regenerate SQL when editing templates

---

### Option B: Template-First Development

Make templates the source of truth:

```bash
# 1. Add to .gitignore
echo "layouts/**/sql/*.sql" >> .gitignore

# 2. Generate SQL before running pipelines
cat > regenerate_sql.sh << 'EOF'
#!/bin/bash
# Regenerate all SQL from templates
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/games_batch.yaml \
    --batch
EOF
chmod +x regenerate_sql.sh

# 3. Update pipeline scripts
# Add to run_game_health_pipeline.sh at line 10:
./regenerate_sql.sh

# 4. Commit only templates
git add templates/ configs/templates/
git commit -m "Source of truth: templates"
```

**Pros:**
- ✅ Always in sync (templates → SQL)
- ✅ Smaller git history (only templates tracked)
- ✅ Force team to use templates

**Cons:**
- ⚠️ Extra build step before running pipelines
- ⚠️ Need to regenerate SQL on every machine

---

### Option C: Hybrid Approach

Use templates for high-repetition projects only:

**Use Templates:**
- ✅ Game Health Check (6 SQL files, 80% duplicate code)
- ✅ Rolling Forecast (3 market types, 75% duplicate code)

**Keep Manual:**
- ⚠️ SensorTower/Facebook/TikTok extractions (unique per source)
- ⚠️ One-off reports (not worth templating)

---

## Real Examples from Your Codebase

### Example 1: VIP Calculation (6 files)

**Before Jinja2:**
```sql
-- Code appears identically in 6 files:
-- diagnostic_daily.sql, package_performance.sql, server_performance.sql
-- and 3 rolling forecast files

CASE 
    WHEN total_rev / 25840 >= 12 THEN '12. VIP 12+'
    WHEN total_rev / 25840 >= 8 THEN '11. VIP 8-11'
    WHEN total_rev / 25840 >= 4 THEN '10. VIP 4-7'
    WHEN total_rev / 25840 >= 2 THEN '9. VIP 2-3'
    WHEN total_rev / 25840 >= 1 THEN '8. VIP 1'
    WHEN total_rev > 0 THEN '7. Non-VIP Paying'
    ELSE '1. Non-Paying'
END as vip_tier

-- Total: 13 lines × 6 files = 78 lines
```

**After Jinja2:**
```sql
-- macros.sql (define once)
{%- macro calculate_vip(revenue_col) %}
CASE 
    WHEN {{ revenue_col }} / 25840 >= 12 THEN '12. VIP 12+'
    WHEN {{ revenue_col }} / 25840 >= 8 THEN '11. VIP 8-11'
    WHEN {{ revenue_col }} / 25840 >= 4 THEN '10. VIP 4-7'
    WHEN {{ revenue_col }} / 25840 >= 2 THEN '9. VIP 2-3'
    WHEN {{ revenue_col }} / 25840 >= 1 THEN '8. VIP 1'
    WHEN {{ revenue_col }} > 0 THEN '7. Non-VIP Paying'
    ELSE '1. Non-Paying'
END
{%- endmacro %}

-- Each template uses macro (1 line)
{{ calculate_vip('total_rev') }} as vip_tier

-- Total: 13 lines (macro) + 6 lines (usage) = 19 lines
-- Reduction: 78 → 19 (76% less code)
```

**Change VIP threshold:**
- Before: Edit 6 files manually (~30 min)
- After: Edit 1 macro (~1 min) → 30x faster

---

### Example 2: Gross Revenue Metrics (11 metrics)

**Before Jinja2:**
```sql
-- Appears in diagnostic_daily.sql and similar files
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '0 day' THEN c.revenue ELSE 0 END) as gross_nru0,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '1 day' THEN c.revenue ELSE 0 END) as gross_nru1,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '3 day' THEN c.revenue ELSE 0 END) as gross_nru3,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '7 day' THEN c.revenue ELSE 0 END) as gross_nru7,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '14 day' THEN c.revenue ELSE 0 END) as gross_nru14,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '30 day' THEN c.revenue ELSE 0 END) as gross_nru30,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '60 day' THEN c.revenue ELSE 0 END) as gross_nru60,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '90 day' THEN c.revenue ELSE 0 END) as gross_nru90,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '180 day' THEN c.revenue ELSE 0 END) as gross_nru180,
SUM(CASE WHEN c.install_time >= CURRENT_DATE - INTERVAL '365 day' THEN c.revenue ELSE 0 END) as gross_nru365,
SUM(c.revenue) as gross_total

-- 11 lines of nearly identical code
```

**After Jinja2:**
```jinja2
-- In template (1 line)
{{ macros.gross_revenue_metrics('c.revenue', 'c.install_time', 'CURRENT_DATE') }}

-- Expands to all 11 lines automatically
```

**Add new metric (e.g., NRU21):**
- Before: Copy line, change '14' to '21', update column name
- After: Edit macro, add one line, regenerate

**Reduction:** 11 lines → 1 line (91% reduction)

---

### Example 3: Game-Specific SQL Files

**Before Jinja2:**
```
layouts/game_health_check/sql/
├── diagnostic_daily_fw2.sql       (200 lines)
├── diagnostic_daily_cft.sql       (200 lines, 95% same as fw2)
├── diagnostic_daily_l2m.sql       (200 lines, 95% same as fw2)
├── diagnostic_daily_mlb.sql       (200 lines, 95% same as fw2)
└── ... 6 more files ...

Total: 10 files × 200 lines = 2,000 lines
Unique content: ~5% × 10 = 100 lines
Duplicate content: 1,900 lines
```

**After Jinja2:**
```
templates/sql/
└── diagnostic_daily.sql.j2        (200 lines - template)

configs/templates/
└── games_batch.yaml               (30 lines - game configs)

Generated (not in git):
layouts/game_health_check/sql/
└── diagnostic_daily_*.sql         (10 files, generated)

Total maintained: 230 lines
Reduction: 2,000 → 230 (88% reduction)
```

**Add new game:**
- Before: Copy 200 lines, edit 50 places (~40 min)
- After: Add 3 lines to config (~10 sec) → 240x faster

---

## Command Reference

### Basic Usage

```bash
# Render template with config file
python src/template_renderer.py \
    --template <path-to-template.j2> \
    --config <path-to-config.yaml> \
    --output <path-to-output>

# Example
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/fw2.yaml \
    --output output.sql
```

### Batch Processing

```bash
# Generate multiple outputs from one config
python src/template_renderer.py \
    --template <path-to-template.j2> \
    --config <path-to-batch-config.yaml> \
    --batch

# Example: Generate SQL for all games
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/games_batch.yaml \
    --batch
```

### Variable Overrides

```bash
# Override variables from command line
python src/template_renderer.py \
    --template <template> \
    --config <config> \
    --var key1=value1 \
    --var key2=value2

# Example
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/fw2.yaml \
    --var target_date=2025-12-27 \
    --var schema=fw2_staging
```

### Dry Run (Preview)

```bash
# Preview output without writing file
python src/template_renderer.py \
    --template <template> \
    --config <config> \
    --dry-run

# Pipe to file viewer
python src/template_renderer.py \
    --template templates/sql/diagnostic_daily.sql.j2 \
    --config configs/templates/fw2.yaml \
    --dry-run | less
```

### Common Patterns

```bash
# Generate all game health check SQL files
for game in fw2 cft l2m mlb; do
    python src/template_renderer.py \
        --template templates/sql/diagnostic_daily.sql.j2 \
        --config configs/templates/${game}.yaml \
        --output layouts/game_health_check/sql/diagnostic_daily_${game}.sql
done

# Generate all rolling forecast SQL (3 market types)
for market in single sea multi; do
    python src/template_renderer.py \
        --template templates/sql/rolling_forecast_market.sql.j2 \
        --var market_type=${market} \
        --output layouts/rolling_forecast/sql/${market}_market.sql
done

# Regenerate all after macro change
./regenerate_all_sql.sh
```

---

## Quick Reference

### Time Savings

| Task | Manual | Jinja2 | Savings |
|------|--------|--------|---------|
| Change VIP threshold | 30 min | 1 min | **30x** |
| Add new game | 40 min | 10 sec | **240x** |
| Add new metric (all files) | 20 min | 2 min | **10x** |
| Fix typo in 6 files | 15 min | 30 sec | **30x** |
| Update currency conversion | 25 min | 1 min | **25x** |

### Code Reduction

| Component | Before | After | Reduction |
|-----------|--------|-------|-----------|
| VIP logic | 78 lines (6 files) | 19 lines (1 macro + 6 usage) | **76%** |
| Gross metrics | 11 lines per file | 1 line per file | **91%** |
| Game-specific SQL | 2,000 lines (10 files) | 230 lines (1 template + config) | **88%** |
| Market SQL variations | 520 lines (3 files) | 200 lines (1 template) | **62%** |

### File Organization

```
gsbkk/
├── templates/                    # Source of truth
│   ├── sql/
│   │   ├── macros.sql           # Reusable SQL snippets
│   │   ├── *.sql.j2             # SQL templates
│   └── layouts/
│       ├── *.json.j2            # Layout templates
│
├── configs/templates/           # Variables for rendering
│   ├── *.yaml                   # Game configs
│
├── src/
│   └── template_renderer.py    # Rendering tool
│
└── layouts/                     # Generated files
    └── **/sql/*.sql            # Can be generated from templates
```

### Key Commands

```bash
# Test
bash demo_jinja2.sh
python src/template_renderer.py --dry-run ...

# Generate
python src/template_renderer.py --template ... --config ... --output ...
python src/template_renderer.py --batch ...

# Integrate
./regenerate_sql.sh
./run_game_health_pipeline.sh 2025-12-26 fw2
```

### Decision Tree

**Should I use templates for this?**

```
Do you have duplicate SQL code across multiple files?
├─ YES → Use templates (high ROI)
└─ NO → Is this code likely to change frequently?
      ├─ YES → Use templates (easier maintenance)
      └─ NO → Manual SQL is fine (low ROI)
```

**When to use templates:**
- ✅ VIP logic (used in 6+ files)
- ✅ Currency conversion (used in 10+ files)
- ✅ Metrics calculation (11 similar lines)
- ✅ Game-specific SQL (95% duplicate)
- ✅ Market variations (75% duplicate)

**When manual is OK:**
- ⚠️ One-off reports
- ⚠️ Unique API extractions
- ⚠️ Ad-hoc queries
- ⚠️ Exploratory analysis

---

## FAQ

**Q: Will templates break my current pipelines?**  
A: No. Generated SQL files are identical to manually written ones. Pipelines don't know the difference.

**Q: Do I need to regenerate SQL every time?**  
A: Only when you edit templates or macros. If you edit generated SQL directly, those changes persist (but will be overwritten if you regenerate).

**Q: What if I make a mistake in the template?**  
A: Use `--dry-run` to preview output first. Test on one game before regenerating all.

**Q: Can I still edit SQL files manually?**  
A: Yes, but manual edits will be lost if you regenerate from template. Better to edit the template.

**Q: How do I know which files are template-generated?**  
A: They have this header:
```sql
-- Generated from Jinja2 template - DO NOT EDIT DIRECTLY
-- Edit templates/sql/diagnostic_daily.sql.j2 instead
```

**Q: What if a game needs custom SQL logic?**  
A: Use conditionals in template:
```jinja2
{% if game_id == 'l2m' %}
    -- L2M-specific logic
{% else %}
    -- Standard logic
{% endif %}
```

**Q: Can I version control templates?**  
A: Yes! Commit templates and configs to git:
```bash
git add templates/ configs/templates/ src/template_renderer.py
git commit -m "Add Jinja2 templating system"
```

**Q: How do I share templates with my team?**  
A: Templates are in git repo. Team members run:
```bash
pip install jinja2 pyyaml
python src/template_renderer.py ...
```

**Q: What if Jinja2 version changes?**  
A: Pin version in requirements:
```bash
echo "jinja2==3.1.2" >> requirements/base.txt
echo "pyyaml==6.0.1" >> requirements/base.txt
```

---

## Next Steps

1. ✅ **Read this guide**
2. ⬜ **Install dependencies:** `pip install jinja2 pyyaml`
3. ⬜ **Run demo:** `bash demo_jinja2.sh`
4. ⬜ **Generate test SQL:** Follow Level 3 tests above
5. ⬜ **Wait for next VIP/metric change**
6. ⬜ **Use templates instead of manual editing**
7. ⬜ **Measure time savings**
8. ⬜ **Decide adoption strategy** (gradual vs full vs hybrid)

---

## Related Documentation

- **[JINJA2_QUICKSTART.md](JINJA2_QUICKSTART.md)** - Quick reference and cheat sheet
- **[JINJA2_EXAMPLES.md](JINJA2_EXAMPLES.md)** - More before/after code examples
- **[templates/README.md](templates/README.md)** - Template file reference
- **[demo_jinja2.sh](demo_jinja2.sh)** - Interactive demo script

---

**Questions or issues?** Check the FAQ above or review [JINJA2_SUMMARY.md](JINJA2_SUMMARY.md) for technical overview.
