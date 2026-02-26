{# SQL Macros - Reusable SQL snippets using Jinja2 #}

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
{%- endmacro %}

{# Currency Conversion - USD #}
{% macro to_usd(amount_column, currency_mapping_alias='cm') %}
{{ amount_column }} * {{ currency_mapping_alias }}.exchange_rate_to_usd
{%- endmacro %}

{# Currency Conversion - VND #}
{% macro to_vnd(amount_column, currency_mapping_alias='cm') %}
{{ amount_column }} * {{ currency_mapping_alias }}.exchange_rate_to_vnd
{%- endmacro %}

{# User Type Classification #}
{% macro user_type(last_login_month_column, current_month_value) %}
CASE 
    WHEN CAST({{ last_login_month_column }} AS DATE) = CAST({{ current_month_value }} AS DATE) THEN 'new'
    ELSE 'old'
END
{%- endmacro %}

{# Currency Mapping with USD Rate - Calculates exchange_rate_to_usd by dividing by USD rate #}
{# Returns currency mapping with exchange_rate_to_usd for all months #}
{% macro latest_usd_rate() %}
(
    SELECT 
        cm.report_month,
        cm.currency_code,
        cm.exchange_rate_to_vnd,
        cm.exchange_rate_to_vnd / usd.exchange_rate_to_vnd as exchange_rate_to_usd
    FROM currency_mapping cm
    INNER JOIN currency_mapping usd 
        ON cm.report_month = usd.report_month 
        AND usd.currency_code = 'USD'
)
{%- endmacro %}

{# Date Filter for ETL #}
{% macro date_filter(date_column, log_date) %}
{% if log_date %}
{{ date_column }} = DATE '{{ log_date }}'
{% else %}
{{ date_column }} >= DATE_SUB(CURRENT_DATE, 30)
{% endif %}
{%- endmacro %}

{# Gross Revenue Metrics - Repetitive SUM calculations #}
{% macro gross_revenue_metrics(table_alias='c', currency_alias='cm') %}
{% for day in [1, 3, 7, 14, 21, 30, 60, 90, 120, 150, 180] %}
SUM({{ table_alias }}.grossnru{{ '%02d' % day }} * {{ currency_alias }}.exchange_rate_to_usd) as GrossNRU{{ '%02d' % day }}{{ ',' if not loop.last else '' }}
{% endfor %}
{%- endmacro %}

{# Gross RPI Metrics #}
{% macro gross_rpi_metrics(table_alias='c', currency_alias='cm') %}
{% for day in [1, 3, 7, 14, 21, 30, 60, 90, 120, 150, 180] %}
SUM({{ table_alias }}.grossrpi{{ '%02d' % day }} * {{ currency_alias }}.exchange_rate_to_usd) as GrossRPI{{ '%02d' % day }}{{ ',' if not loop.last else '' }}
{% endfor %}
{%- endmacro %}

{# Country Filter #}
{% macro country_filter(country_code) %}
{% if country_code %}
AND country_code = '{{ country_code }}'
{% endif %}
{%- endmacro %}

{# Game-specific table reference #}
{% macro game_table(table_name, game_id, schema='staging') %}
{{ schema }}.{{ table_name }}_{{ game_id }}
{%- endmacro %}
