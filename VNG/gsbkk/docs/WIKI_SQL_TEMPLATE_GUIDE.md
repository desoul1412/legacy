# SQL Template & Jinja2 Macros - Complete Guide

**Purpose:** Create reusable, maintainable SQL templates using Jinja2 templating engine for data transformations.

---

## 📋 Table of Contents
1. [Overview](#overview)
2. [Why Use SQL Templates](#why-use-sql-templates)
3. [Template Structure](#template-structure)
4. [Variables](#variables)
5. [Macros](#macros)
6. [Conditionals](#conditionals)
7. [Loops](#loops)
8. [Complete Examples](#complete-examples)
9. [Standard Macros Reference](#standard-macros-reference)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Overview

### What is Jinja2?

**Jinja2** is a templating engine that allows you to generate SQL dynamically using:
- **Variables:** Replace hardcoded values with `{{ variable_name }}`
- **Macros:** Reusable SQL snippets like functions
- **Conditionals:** Different SQL for different scenarios (`{% if %}`)
- **Loops:** Generate repetitive code (`{% for %}`)

### The Problem Without Templates

**Before (Repetitive Code):**

You have the same VIP calculation in 6 different SQL files:

```sql
-- diagnostic_daily.sql
CASE 
    WHEN total_rev / 25840 >= 20000 THEN 'G. >=20,000$'
    WHEN total_rev / 25840 >= 4000 THEN 'F. 4,000$ - 20,000$'
    -- ... 6 more lines
END as vip_tier

-- package_performance.sql  
CASE 
    WHEN total_rev / 25840 >= 20000 THEN 'G. >=20,000$'
    WHEN total_rev / 25840 >= 4000 THEN 'F. 4,000$ - 20,000$'
    -- ... same 6 lines again
END as vip_tier

-- (repeat in 4 more files...)
```

**Impact when changing VIP threshold:**
- ❌ Edit 6 files manually (30 minutes)
- ❌ Risk missing a file or making typos
- ❌ Difficult to ensure consistency
- ❌ 1,200 lines of duplicated code

### The Solution With Templates

**After (Macro Once, Use Everywhere):**

**Define macro once** (`templates/sql/macros.sql`):
```sql
{% macro calculate_vip(revenue_col) %}
CASE 
    WHEN {{ revenue_col }} / 25840 >= 20000 THEN 'G. >=20,000$'
    WHEN {{ revenue_col }} / 25840 >= 4000 THEN 'F. 4,000$ - 20,000$'
    WHEN {{ revenue_col }} / 25840 >= 2000 THEN 'E. 2,000$ - 4000$'
    WHEN {{ revenue_col }} / 25840 >= 400 THEN 'D. 400$ - 2000$'
    WHEN {{ revenue_col }} / 25840 >= 200 THEN 'C. 200$ - 400$'
    WHEN {{ revenue_col }} / 25840 >= 40 THEN 'B. 40$ - 200$'
    ELSE 'A. <40$'
END
{% endmacro %}
```

**Use everywhere** (`templates/game_health_check/diagnostic_daily.sql.j2`):
```sql
{% import 'macros.sql' as macros %}

SELECT 
    user_id,
    {{ macros.calculate_vip('total_rev') }} AS vip_tier
FROM users
```

**Benefits:**
- ✅ Edit once (1 minute)
- ✅ Automatically updates all 6 files
- ✅ 200 lines instead of 1,200 lines (83% reduction)
- ✅ **240x faster** when adding new game

---

## Why Use SQL Templates

### Comparison Table

| Task | Manual SQL | SQL Templates | Savings |
|------|------------|---------------|---------|
| **Change VIP threshold** | Edit 6 files (~30 min) | Edit 1 macro (~1 min) | **30x faster** |
| **Add new game** | Copy 200 lines, edit 50 places (~40 min) | Render template (~10 sec) | **240x faster** |
| **Add currency conversion** | Update 3 files (~20 min) | Update 1 macro (~2 min) | **10x faster** |
| **Code to maintain** | 6 files × 200 lines = 1,200 lines | 1 template × 200 lines = 200 lines | **83% less code** |

### When to Use Templates

✅ **Use templates when:**
- Same logic appears in multiple SQL files (macros)
- SQL varies by game, market, or date (variables)
- Complex, repetitive calculations (loops)
- Different queries for different scenarios (conditionals)

❌ **Use plain SQL when:**
- Simple, one-time query
- No repetition or variation
- No complex logic

---

## Template Structure

### Basic Template File

**File:** `templates/game_health_check/diagnostic_daily.sql.j2`

```jinja
{# Comment: This is a Jinja2 comment (not in output SQL) #}

{% import 'macros.sql' as macros %}

-- Generated SQL starts here
SELECT 
    user_id,
    report_date,
    {{ macros.calculate_vip('total_rev') }} AS vip_tier
FROM users
WHERE report_date = '{{ log_date }}'
  AND game_id = '{{ game_id }}'
```

**Components:**
1. **Jinja2 comments:** `{# ... #}` (removed from output)
2. **Import macros:** `{% import 'macros.sql' as macros %}`
3. **Variables:** `{{ variable_name }}`
4. **Macro calls:** `{{ macros.macro_name('arg') }}`
5. **SQL code:** Regular SQL

### File Extension

All templates use `.sql.j2` extension:
- `.sql` = Output is SQL
- `.j2` = Input is Jinja2 template

### Template Search Paths

ETL engine searches for templates and macros in:

```
templates/
├── sql/
│   └── macros.sql           # Shared macros (imported by all)
├── game_health_check/
│   ├── diagnostic_daily.sql.j2
│   └── package_performance.sql.j2
└── rolling_forecast/
    └── rolling_forecast_market.sql.j2
```

**Import paths:**
```jinja
{% import 'macros.sql' as macros %}         {# From templates/sql/ #}
{% import 'sql/macros.sql' as macros %}     {# Also works #}
{% import '../sql/macros.sql' as macros %}  {# Relative path #}
```

---

## Variables

### Using Variables

**Syntax:**
```jinja
{{ variable_name }}
```

**Example:**
```sql
SELECT * 
FROM {{ game_id }}.active_users 
WHERE ds = '{{ log_date }}'
  AND country_code = '{{ country_code }}'
```

**Rendered output (when `game_id=fw2`, `log_date=2026-01-07`, `country_code=TH`):**
```sql
SELECT * 
FROM fw2.active_users 
WHERE ds = '2026-01-07'
  AND country_code = 'TH'
```

### Variable Sources

Variables come from:

#### 1. Runtime Variables (Command Line)

```bash
./run_etl_process.sh \
  --layout layout.json \
  --vars "gameId=fw2,logDate=2026-01-07"
```

**Available in templates as:**
- `{{ game_id }}` (auto-converted from `gameId`)
- `{{ log_date }}` (auto-converted from `logDate`)

#### 2. Layout Variables

**Layout file:**
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

**Template:**
```sql
WHERE market = '{{ market_type }}'
  AND country_code = '{{ country_code }}'
  AND revenue / {{ vip_threshold }} >= 1
```

### Variable Naming Convention

| Layout JSON | SQL Template | Description |
|-------------|--------------|-------------|
| `gameId` | `game_id` | Auto-converted |
| `logDate` | `log_date` | Auto-converted |
| `market_type` | `market_type` | Already snake_case |
| `countryCode` | `country_code` | Auto-converted |

**Rule:** Layout uses `camelCase`, templates use `snake_case`. ETL engine auto-converts.

### Default Values

Provide defaults if variable might be missing:

```jinja
{{ game_id | default('fw2') }}
{{ country_code | default('TH') }}
```

### Variable Arithmetic

```jinja
{{ revenue * 1.1 }}                    {# Multiply #}
{{ total_rev / vip_threshold }}        {# Divide #}
{{ price | round(2) }}                 {# Round to 2 decimals #}
```

---

## Macros

### What are Macros?

Macros are **reusable SQL snippets** - like functions but for SQL code.

### Defining Macros

**File:** `templates/sql/macros.sql`

```sql
{# VIP Level Calculation - Standardized across all games #}
{% macro calculate_vip(total_rev_column='total_rev') %}
CASE 
    WHEN {{ total_rev_column }} = 0 THEN 'Free'
    WHEN {{ total_rev_column }} / 25840 >= 20000 THEN 'G. >=20,000$'
    WHEN {{ total_rev_column }} / 25840 >= 4000 THEN 'F. 4,000$ - 20,000$'
    WHEN {{ total_rev_column }} / 25840 >= 2000 THEN 'E. 2,000$ - 4000$'
    WHEN {{ total_rev_column }} / 25840 >= 400 THEN 'D. 400$ - 2000$'
    WHEN {{ total_rev_column }} / 25840 >= 200 THEN 'C. 200$ - 400$'
    WHEN {{ total_rev_column }} / 25840 >= 40 THEN 'B. 40$ - 200$'
    ELSE 'A. <40$'
END
{% endmacro %}
```

**Syntax:**
```jinja
{% macro macro_name(param1, param2='default_value') %}
    SQL code with {{ param1 }} and {{ param2 }}
{% endmacro %}
```

### Using Macros

**Import first:**
```jinja
{% import 'macros.sql' as macros %}
```

**Call macro:**
```sql
SELECT 
    user_id,
    {{ macros.calculate_vip('total_rev') }} AS vip_tier,
    {{ macros.calculate_vip('prev_revenue') }} AS prev_vip_tier
FROM users
```

**Rendered output:**
```sql
SELECT 
    user_id,
    CASE 
        WHEN total_rev = 0 THEN 'Free'
        WHEN total_rev / 25840 >= 20000 THEN 'G. >=20,000$'
        -- ... rest of CASE
    END AS vip_tier,
    CASE 
        WHEN prev_revenue = 0 THEN 'Free'
        WHEN prev_revenue / 25840 >= 20000 THEN 'G. >=20,000$'
        -- ... rest of CASE
    END AS prev_vip_tier
FROM users
```

### Macro Parameters

#### Positional Parameters

```jinja
{% macro date_filter(date_column, log_date) %}
{{ date_column }} = DATE '{{ log_date }}'
{% endmacro %}
```

**Usage:**
```sql
WHERE {{ macros.date_filter('report_date', '2026-01-07') }}
```

**Output:**
```sql
WHERE report_date = DATE '2026-01-07'
```

#### Named Parameters with Defaults

```jinja
{% macro to_usd(amount_column, currency_mapping_alias='cm', latest_rate_alias='latest') %}
{{ amount_column }} * COALESCE({{ currency_mapping_alias }}.exchange_rate_to_usd, 
                               {{ latest_rate_alias }}.latest_rate_usd)
{% endmacro %}
```

**Usage:**
```sql
SELECT {{ macros.to_usd('revenue') }} AS revenue_usd
```

**Output:**
```sql
SELECT revenue * COALESCE(cm.exchange_rate_to_usd, latest.latest_rate_usd) AS revenue_usd
```

### Multi-Line Macros

Macros can contain any SQL:

```jinja
{% macro latest_usd_rate() %}
(
    SELECT exchange_rate_to_usd
    FROM currency_mapping
    WHERE currency_code = 'USD'
    ORDER BY report_month DESC
    LIMIT 1
)
{% endmacro %}
```

**Usage:**
```sql
CROSS JOIN {{ macros.latest_usd_rate() }} latest
```

**Output:**
```sql
CROSS JOIN (
    SELECT exchange_rate_to_usd
    FROM currency_mapping
    WHERE currency_code = 'USD'
    ORDER BY report_month DESC
    LIMIT 1
) latest
```

---

## Conditionals

### If-Else Statements

**Syntax:**
```jinja
{% if condition %}
    SQL for true case
{% elif other_condition %}
    SQL for other case
{% else %}
    SQL for false case
{% endif %}
```

### Example: Market-Specific Logic

**Template:**
```jinja
{% import 'macros.sql' as macros %}

SELECT 
    user_id,
    report_date,
    {% if market_type == 'single' %}
        -- Single market: revenue already in USD
        revenue AS revenue_usd
    {% elif market_type == 'sea' %}
        -- SEA: convert local currency to USD
        {{ macros.to_usd('revenue', 'cm', 'latest') }} AS revenue_usd
    {% else %}
        -- Multi-country: aggregate by country
        SUM({{ macros.to_usd('revenue', 'cm', 'latest') }}) AS revenue_usd
    {% endif %}
FROM active
{% if country_code %}
WHERE country_code = '{{ country_code }}'
{% endif %}
GROUP BY user_id, report_date
```

**When `market_type=single`, `country_code=TH`:**
```sql
SELECT 
    user_id,
    report_date,
    revenue AS revenue_usd
FROM active
WHERE country_code = 'TH'
GROUP BY user_id, report_date
```

**When `market_type=sea`, no `country_code`:**
```sql
SELECT 
    user_id,
    report_date,
    revenue * COALESCE(cm.exchange_rate_to_usd, latest.latest_rate_usd) AS revenue_usd
FROM active
GROUP BY user_id, report_date
```

### Conditional Operators

```jinja
{% if var == 'value' %}         {# Equality #}
{% if var != 'value' %}         {# Inequality #}
{% if var > 100 %}              {# Greater than #}
{% if var and other_var %}      {# AND #}
{% if var or other_var %}       {# OR #}
{% if not var %}                {# NOT #}
{% if var in ['a', 'b', 'c'] %} {# IN list #}
{% if var is defined %}         {# Variable exists #}
```

---

## Loops

### For Loops

**Syntax:**
```jinja
{% for item in list %}
    SQL with {{ item }}
{% endfor %}
```

### Example: Generate Repetitive Columns

**Problem:** You need columns like `GrossNRU01`, `GrossNRU03`, `GrossNRU07`, ..., `GrossNRU180`

**Without loops (manual):**
```sql
SUM(c.grossnru01 * cm.exchange_rate_to_usd) as GrossNRU01,
SUM(c.grossnru03 * cm.exchange_rate_to_usd) as GrossNRU03,
SUM(c.grossnru07 * cm.exchange_rate_to_usd) as GrossNRU07,
-- ... 8 more lines
```

**With loops (macro):**
```jinja
{% macro gross_revenue_metrics(table_alias='c', currency_alias='cm') %}
{% for day in [1, 3, 7, 14, 21, 30, 60, 90, 120, 150, 180] %}
SUM({{ table_alias }}.grossnru{{ '%02d' % day }} * {{ currency_alias }}.exchange_rate_to_usd) as GrossNRU{{ '%02d' % day }}{{ ',' if not loop.last else '' }}
{% endfor %}
{% endmacro %}
```

**Usage:**
```sql
SELECT 
    user_id,
    {{ macros.gross_revenue_metrics('c', 'cm') }}
FROM charge c
```

**Output:**
```sql
SELECT 
    user_id,
    SUM(c.grossnru01 * cm.exchange_rate_to_usd) as GrossNRU01,
    SUM(c.grossnru03 * cm.exchange_rate_to_usd) as GrossNRU03,
    SUM(c.grossnru07 * cm.exchange_rate_to_usd) as GrossNRU07,
    SUM(c.grossnru14 * cm.exchange_rate_to_usd) as GrossNRU14,
    SUM(c.grossnru21 * cm.exchange_rate_to_usd) as GrossNRU21,
    SUM(c.grossnru30 * cm.exchange_rate_to_usd) as GrossNRU30,
    SUM(c.grossnru60 * cm.exchange_rate_to_usd) as GrossNRU60,
    SUM(c.grossnru90 * cm.exchange_rate_to_usd) as GrossNRU90,
    SUM(c.grossnru120 * cm.exchange_rate_to_usd) as GrossNRU120,
    SUM(c.grossnru150 * cm.exchange_rate_to_usd) as GrossNRU150,
    SUM(c.grossnru180 * cm.exchange_rate_to_usd) as GrossNRU180
FROM charge c
```

### Loop Variables

```jinja
{% for item in list %}
    {{ item }}              {# Current item #}
    {{ loop.index }}        {# Current iteration (1-based) #}
    {{ loop.index0 }}       {# Current iteration (0-based) #}
    {{ loop.first }}        {# True if first iteration #}
    {{ loop.last }}         {# True if last iteration #}
{% endfor %}
```

**Example: Comma handling**
```jinja
{% for col in ['user_id', 'revenue', 'country_code'] %}
{{ col }}{{ ',' if not loop.last else '' }}
{% endfor %}
```

**Output:**
```
user_id,
revenue,
country_code
```

---

## Complete Examples

### Example 1: VIP Calculation with Macro

**Macro definition** (`templates/sql/macros.sql`):
```sql
{% macro calculate_vip(total_rev_column='total_rev') %}
CASE 
    WHEN {{ total_rev_column }} = 0 THEN 'Free'
    WHEN {{ total_rev_column }} / 25840 >= 20000 THEN 'G. >=20,000$'
    WHEN {{ total_rev_column }} / 25840 >= 4000 THEN 'F. 4,000$ - 20,000$'
    WHEN {{ total_rev_column }} / 25840 >= 2000 THEN 'E. 2,000$ - 4000$'
    WHEN {{ total_rev_column }} / 25840 >= 400 THEN 'D. 400$ - 2000$'
    WHEN {{ total_rev_column }} / 25840 >= 200 THEN 'C. 200$ - 400$'
    WHEN {{ total_rev_column }} / 25840 >= 40 THEN 'B. 40$ - 200$'
    ELSE 'A. <40$'
END
{% endmacro %}
```

**Template** (`templates/game_health_check/diagnostic_daily.sql.j2`):
```jinja
{% import 'macros.sql' as macros %}

WITH user_profile_vip AS (
    SELECT 
        game_id,
        user_id,
        country_code,
        {{ macros.calculate_vip('total_rev') }} AS vip_level
    FROM user_profile
    WHERE game_id = '{{ game_id }}'
)

SELECT * FROM user_profile_vip
```

**Layout** (`layouts/game_health_check/cons/diagnostic_daily.json`):
```json
{
  "inputSources": [{
    "name": "user_profile",
    "type": "jdbc",
    "connection": "GDS_POSTGRES",
    "table": "fw2.user_profile"
  }],
  "sqlFile": "templates/game_health_check/diagnostic_daily.sql.j2",
  "outputs": [{ /* ... */ }]
}
```

**Usage:**
```bash
./run_etl_process.sh \
  --layout layouts/game_health_check/cons/diagnostic_daily.json \
  --vars "gameId=fw2,logDate=2026-01-07"
```

### Example 2: Currency Conversion with Variables

**Macro** (`templates/sql/macros.sql`):
```sql
{% macro to_usd(amount_column, currency_mapping_alias='cm', latest_rate_alias='latest') %}
{{ amount_column }} * COALESCE({{ currency_mapping_alias }}.exchange_rate_to_usd, 
                               {{ latest_rate_alias }}.latest_rate_usd)
{% endmacro %}

{% macro latest_usd_rate() %}
(
    SELECT exchange_rate_to_usd
    FROM currency_mapping
    WHERE currency_code = 'USD'
    ORDER BY report_month DESC
    LIMIT 1
)
{% endmacro %}
```

**Template:**
```jinja
{% import 'macros.sql' as macros %}

WITH currency_with_usd AS (
    SELECT 
        cm.*,
        latest.exchange_rate_to_usd as latest_rate_usd
    FROM currency_mapping cm
    CROSS JOIN {{ macros.latest_usd_rate() }} latest
)

SELECT
    c.user_id,
    c.report_date,
    {{ macros.to_usd('c.revenue', 'cm', 'latest') }} as revenue_usd,
    {{ macros.to_usd('c.grossrev_npu', 'cm', 'latest') }} as gross_npu_usd
FROM charge_details c
LEFT JOIN currency_with_usd cm
    ON c.currency_code = cm.currency_code
WHERE c.report_date = '{{ log_date }}'
```

### Example 3: Market-Specific Logic with Conditionals

**Template** (`templates/rolling_forecast/rolling_forecast_market.sql.j2`):
```jinja
{% import 'macros.sql' as macros %}

-- Rolling Forecast: {{ market_type }} market
-- Generated for game: {{ game_id }}, date: {{ log_date }}

SELECT 
    report_date,
    user_id,
    
    {% if market_type == 'single' %}
        -- Single market (Thailand only)
        '{{ country_code }}' AS country_code,
        revenue AS revenue_usd  -- Already in USD from game_health_check
        
    {% elif market_type == 'sea' %}
        -- SEA region aggregate
        country_code,
        SUM({{ macros.to_usd('revenue', 'cm', 'latest') }}) AS revenue_usd
        
    {% elif market_type == 'multi' %}
        -- Multi-country per-country metrics
        country_code,
        {{ macros.to_usd('revenue', 'cm', 'latest') }} AS revenue_usd,
        country_name
        
    {% else %}
        -- Default: assume single market
        revenue AS revenue_usd
    {% endif %}

FROM active a
LEFT JOIN retention r ON a.user_id = r.user_id
LEFT JOIN charge c ON a.user_id = c.user_id

{% if market_type == 'single' %}
WHERE a.country_code = '{{ country_code }}'
{% endif %}

GROUP BY 
    report_date,
    user_id
    {% if market_type in ['sea', 'multi'] %}
    , country_code
    {% endif %}
    {% if market_type == 'multi' %}
    , country_name
    {% endif %}
```

**Layouts for different markets:**

**Single market** (`layouts/rolling_forecast/cons/single_market.json`):
```json
{
  "sqlFile": "templates/rolling_forecast/rolling_forecast_market.sql.j2",
  "variables": {
    "market_type": "single",
    "country_code": "TH"
  }
}
```

**SEA market** (`layouts/rolling_forecast/cons/sea_market.json`):
```json
{
  "sqlFile": "templates/rolling_forecast/rolling_forecast_market.sql.j2",
  "variables": {
    "market_type": "sea"
  }
}
```

**Multi-country** (`layouts/rolling_forecast/cons/multi_country.json`):
```json
{
  "sqlFile": "templates/rolling_forecast/rolling_forecast_market.sql.j2",
  "variables": {
    "market_type": "multi"
  }
}
```

### Example 4: Loop to Generate Repetitive Metrics

**Macro** (`templates/sql/macros.sql`):
```sql
{% macro gross_revenue_metrics(table_alias='c', currency_alias='cm', latest_alias='latest') %}
{% for day in [1, 3, 7, 14, 21, 30, 60, 90, 120, 150, 180] %}
SUM({{ table_alias }}.grossnru{{ '%02d' % day }} * COALESCE({{ currency_alias }}.exchange_rate_to_usd, {{ latest_alias }}.latest_rate_usd)) as GrossNRU{{ '%02d' % day }}{{ ',' if not loop.last else '' }}
{% endfor %}
{% endmacro %}

{% macro gross_rpi_metrics(table_alias='c', currency_alias='cm', latest_alias='latest') %}
{% for day in [1, 3, 7, 14, 21, 30, 60, 90, 120, 150, 180] %}
SUM({{ table_alias }}.grossrpi{{ '%02d' % day }} * COALESCE({{ currency_alias }}.exchange_rate_to_usd, {{ latest_alias }}.latest_rate_usd)) as GrossRPI{{ '%02d' % day }}{{ ',' if not loop.last else '' }}
{% endfor %}
{% endmacro %}
```

**Usage:**
```jinja
{% import 'macros.sql' as macros %}

SELECT 
    c.game_id,
    c.report_date,
    {{ macros.gross_revenue_metrics('c', 'cm', 'latest') }},
    {{ macros.gross_rpi_metrics('c', 'cm', 'latest') }}
FROM charge_details c
LEFT JOIN currency_mapping cm ON c.currency_code = cm.currency_code
```

**Rendered output:** 22 columns automatically generated!

---

## Standard Macros Reference

### Available Macros in `templates/sql/macros.sql`

#### 1. `calculate_vip(total_rev_column='total_rev')`

**Purpose:** Calculate VIP tier based on total revenue.

**Parameters:**
- `total_rev_column`: Column name with total revenue (default: `'total_rev'`)

**Returns:** CASE expression with VIP tiers (Free, A-G)

**Example:**
```sql
{{ macros.calculate_vip('total_rev') }} AS vip_level
{{ macros.calculate_vip('prev_total_rev') }} AS prev_vip_level
```

---

#### 2. `to_usd(amount_column, currency_mapping_alias='cm', latest_rate_alias='latest')`

**Purpose:** Convert local currency to USD using exchange rates.

**Parameters:**
- `amount_column`: Column with amount to convert
- `currency_mapping_alias`: Alias for currency_mapping table (default: `'cm'`)
- `latest_rate_alias`: Alias for latest rate CTE (default: `'latest'`)

**Returns:** Expression for USD conversion

**Example:**
```sql
{{ macros.to_usd('revenue') }} AS revenue_usd
{{ macros.to_usd('c.grossrev_npu', 'cm', 'latest') }} AS gross_npu_usd
```

---

#### 3. `to_vnd(amount_column, currency_mapping_alias='cm', latest_rate_alias='latest')`

**Purpose:** Convert local currency to VND.

**Parameters:** Same as `to_usd`

**Example:**
```sql
{{ macros.to_vnd('revenue') }} AS revenue_vnd
```

---

#### 4. `latest_usd_rate()`

**Purpose:** Subquery to get latest USD exchange rate.

**Parameters:** None

**Returns:** Subquery `(SELECT ... FROM currency_mapping ... LIMIT 1)`

**Example:**
```sql
CROSS JOIN {{ macros.latest_usd_rate() }} latest
```

---

#### 5. `date_filter(date_column, log_date)`

**Purpose:** Generate date filter WHERE clause.

**Parameters:**
- `date_column`: Column to filter
- `log_date`: Date value (if empty, uses 30-day window)

**Returns:** Date comparison expression

**Example:**
```sql
WHERE {{ macros.date_filter('report_date', '2026-01-07') }}
-- Output: WHERE report_date = DATE '2026-01-07'

WHERE {{ macros.date_filter('ds', log_date) }}
-- With variable
```

---

#### 6. `gross_revenue_metrics(table_alias='c', currency_alias='cm', latest_alias='latest')`

**Purpose:** Generate all GrossNRU metrics (01, 03, 07, ..., 180) with USD conversion.

**Parameters:**
- `table_alias`: Alias for charge table (default: `'c'`)
- `currency_alias`: Alias for currency_mapping (default: `'cm'`)
- `latest_alias`: Alias for latest rate (default: `'latest'`)

**Returns:** 11 SUM expressions (GrossNRU01 through GrossNRU180)

**Example:**
```sql
SELECT 
    game_id,
    {{ macros.gross_revenue_metrics('c', 'cm', 'latest') }}
FROM charge c
```

---

#### 7. `gross_rpi_metrics(table_alias='c', currency_alias='cm', latest_alias='latest')`

**Purpose:** Generate all GrossRPI metrics (01, 03, 07, ..., 180) with USD conversion.

**Parameters:** Same as `gross_revenue_metrics`

**Example:**
```sql
{{ macros.gross_rpi_metrics('c', 'cm', 'latest') }}
```

---

#### 8. `country_filter(country_code)`

**Purpose:** Add country filter if country_code is provided.

**Parameters:**
- `country_code`: Country code or empty

**Returns:** `AND country_code = 'XX'` or empty string

**Example:**
```sql
WHERE 1=1
  {{ macros.country_filter('TH') }}
-- Output: WHERE 1=1 AND country_code = 'TH'
```

---

#### 9. `game_table(table_name, game_id, schema='staging')`

**Purpose:** Generate game-specific table name.

**Parameters:**
- `table_name`: Base table name
- `game_id`: Game identifier
- `schema`: Schema name (default: `'staging'`)

**Returns:** `schema.table_name_game_id`

**Example:**
```sql
FROM {{ macros.game_table('active_users', 'fw2', 'public') }}
-- Output: FROM public.active_users_fw2
```

---

## Best Practices

### 1. Macro Organization

✅ **DO:**
- Keep all shared macros in `templates/sql/macros.sql`
- One macro per logical function (VIP, currency, date filter, etc.)
- Document macro parameters with comments
- Use descriptive macro names (`calculate_vip`, not `vip`)

❌ **DON'T:**
- Duplicate macros across multiple files
- Create project-specific macros in `macros.sql` (use project template folder)
- Use cryptic names (`m1`, `calc`, `fn`)

### 2. Variable Naming

✅ **DO:**
- Use `snake_case` in SQL templates
- Use descriptive names (`market_type`, not `m`)
- Provide defaults with `| default('value')`

❌ **DON'T:**
- Mix `camelCase` and `snake_case` in templates
- Use single-letter variables (`x`, `y`)
- Assume variables always exist

### 3. Conditionals

✅ **DO:**
- Use meaningful condition names
- Document what each branch does
- Keep conditional logic simple
- Indent SQL inside conditionals

❌ **DON'T:**
- Nest more than 2 levels deep
- Put complex logic in conditions
- Leave branches undocumented

### 4. Loops

✅ **DO:**
- Use loops for truly repetitive patterns
- Keep loop body simple
- Handle commas with `{{ ',' if not loop.last else '' }}`

❌ **DON'T:**
- Use loops for 1-2 items (just write them out)
- Put complex SQL in loop body
- Forget comma handling

### 5. Comments

✅ **DO:**
- Use Jinja2 comments `{# ... #}` for template logic
- Use SQL comments `-- ...` for SQL documentation
- Document macro parameters
- Explain complex conditionals

❌ **DON'T:**
- Over-comment obvious code
- Leave template logic unexplained

### 6. Testing

✅ **DO:**
- Test templates with different variable combinations
- Validate generated SQL syntax
- Test edge cases (empty variables, nulls)
- Use small datasets for initial testing

❌ **DON'T:**
- Deploy untested templates
- Assume all variable combinations work
- Skip SQL validation

---

## Troubleshooting

### Common Errors

#### 1. Template Not Found

**Error:**
```
jinja2.exceptions.TemplateNotFound: macros.sql
```

**Fix:**
- Check import path: `{% import 'macros.sql' as macros %}`
- Ensure `templates/sql/macros.sql` exists
- Verify template search paths in ETL engine

#### 2. Variable Not Defined

**Error:**
```
jinja2.exceptions.UndefinedError: 'game_id' is undefined
```

**Fix:**
- Pass variable in command: `--vars "gameId=fw2"`
- Add default: `{{ game_id | default('fw2') }}`
- Check variable name spelling (case-sensitive)

#### 3. Syntax Error in Rendered SQL

**Error:**
```
pyspark.sql.utils.ParseException: Syntax error at or near 'AND'
```

**Fix:**
- Print rendered SQL for debugging:
  ```bash
  ./run_etl_process.sh --layout layout.json --vars "..." --dry-run
  ```
- Check for extra/missing commas in loops
- Validate conditional branches

#### 4. Macro Returns Wrong Output

**Error:**
```
Expected: revenue_usd
Got: {{ macros.to_usd('revenue') }}
```

**Fix:**
- Check you imported macros: `{% import 'macros.sql' as macros %}`
- Verify macro call syntax: `{{ macros.macro_name('arg') }}`
- Ensure template file has `.j2` extension

#### 5. Whitespace Issues

**Error:**
```
Unexpected extra spaces in generated SQL
```

**Fix:**
- Use `-` to strip whitespace:
  ```jinja
  {%- if condition -%}    {# Strip before and after #}
  {%- endmacro %}         {# Strip trailing whitespace #}
  ```

### Debugging Tips

#### 1. Print Rendered SQL

**Method 1: Dry run (if supported)**
```bash
./run_etl_process.sh --layout layout.json --vars "gameId=fw2,logDate=2026-01-07" --dry-run
```

**Method 2: Manual rendering**
```python
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader('templates'))
template = env.get_template('game_health_check/diagnostic_daily.sql.j2')
print(template.render(game_id='fw2', log_date='2026-01-07'))
```

#### 2. Test Macros Independently

Create test template:
```jinja
{% import 'macros.sql' as macros %}

-- Test VIP calculation
SELECT {{ macros.calculate_vip('total_rev') }} AS vip_tier

-- Test currency conversion
SELECT {{ macros.to_usd('revenue', 'cm', 'latest') }} AS revenue_usd
```

Render and verify output.

#### 3. Validate Variable Substitution

Add debug output:
```jinja
{# Debug: game_id = {{ game_id }}, log_date = {{ log_date }} #}

SELECT * FROM {{ game_id }}.table WHERE ds = '{{ log_date }}'
```

Check rendered SQL has correct values.

---

## Next Steps

📚 **Related Documentation:**
- [Layout Guide](WIKI_LAYOUT_GUIDE.md) - How to create layout files
- [Re-standardization Guide](WIKI_RESTANDARDIZATION_GUIDE.md) - Adding new games
- [Jinja2 Documentation](https://jinja.palletsprojects.com/) - Official Jinja2 docs

🎯 **Quick Actions:**
- Review existing macros: `templates/sql/macros.sql`
- Create your first template: Copy `templates/game_health_check/diagnostic_daily.sql.j2`
- Test rendering: Use Python snippet above
- Add new macro: Edit `templates/sql/macros.sql`

---

**Last Updated:** 2026-01-07  
**Maintainer:** Data Engineering Team
