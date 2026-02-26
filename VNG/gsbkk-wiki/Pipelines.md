# Pipelines

This page summarizes the key scripts and DAGs in the GSBKK repository, grouped by functionality.

## Overview

The GSBKK repository contains data pipelines for game analytics, built with Apache Airflow and Spark.

## Pipeline Groups

### Rolling Forecast Pipelines

Daily revenue forecasting and user metrics for games.

#### DAGs

The following DAGs run daily rolling forecasts for various games:

- \cft_rolling_forecast_dag.py\
- \cloudsong_rolling_forecast_dag.py\
- \w2_rolling_forecast_dag.py\ (example: runs \olling_forecast_full.py fw2\)
- \gnoth_rolling_forecast_dag.py\
- \mlb_rolling_forecast_dag.py\
- \mts_rolling_forecast_dag.py\
- \omgthai_rolling_forecast_dag.py\
- \pwmsea_rolling_forecast_dag.py\
- \slth_rolling_forecast_dag.py\
- \	d_rolling_forecast_dag.py\

Each runs a Spark job with \olling_forecast_full.py\ or similar scripts.

#### Scripts

- **rolling_forecast_full.py**: Full rolling forecast processing, extracts data from Postgres, processes with Spark, updates Google Sheets
- **rolling_forecast_inc.py**: Incremental rolling forecast

#### Data Flow

1. **Data Extraction**: Pull active users, charges, retention rates from GDS Postgres.
2. **User Metrics Calculation**:
   - Aggregate installs, new users, daily active users, paying users, etc.
   - Calculate retention rates (RR01, RR07).
3. **Revenue Processing**:
   - Convert all revenues to USD and VND using monthly exchange rates.
   - Aggregate daily revenue by country.
4. **Final Aggregation**: Join user metrics with revenue data.
5. **Output**: Update game-specific Google Sheet with daily KPIs.

#### Game-Specific Logic

- **SLTH**: Country fixed to "TH"
- **PWMSEA/OMGTHAI/GNOTH**: Country fixed to "SEA", with special handling for PWMSEA to include TH and other countries
- **CFT/FW2/MLB/MTS**: Use country_code directly
- **Others**: Default grouping

#### Dependencies

- \unctions.constants\: Credentials and paths
- \unctions.data_extraction\: Data pulling
- \unctions.warehouse\: Currency mapping and DB writes
- \pyspark\: Data processing
- \googleapiclient\: Sheet updates

#### Scheduling

All DAGs run daily at 7 AM, processing previous day's data.

### Monthly Data.AI Pipeline

Monthly market data processing and revenue estimation.

#### DAG

\monthly_data.ai_dag.py\: Runs on the 5th of each month at 3 AM for countries: VN, TH, PH, ID, MX, SG.

Each task calls \un_spark_job.sh monthly_data.ai.py {country}\.

#### Script

**monthly_data.ai.py**: Main script for processing monthly data.ai.

##### Functions

- \get_spreadsheet_ids()\: Searches Google Drive for spreadsheets matching "data.ai_{country}_{year-month}" pattern, returns sheet IDs.
- \extract_data(country, raw_sheet_id)\: Reads data from Sheet1!A11:BF1012 of the raw sheet, maps columns using \column_mapping\, adds Report Month and Market.
- \country_config(country)\: Returns genre and company revenue estimation configs for the country.
- \monthly_transform(country, raw_data)\: 
  - Creates Spark DataFrame from raw data
  - Splits complex columns (e.g., "Art Style & Orientation" -> art_style, orientation, etc.)
  - Estimates revenue based on country-specific genre/company multipliers
  - Adds processed columns
- \gg_transform(monthly_sheet_id)\: Aggregates monthly data across countries, calculates totals.
- \ppend(target_sheet_id, data, type)\: Appends processed data to target Google Sheet.
- \execute(country)\: Orchestrates: get sheet IDs, extract, transform monthly, append to monthly sheet, then aggregate and append to agg sheet.

##### Data Flow

1. **Sheet Discovery**: Find raw data sheets for the current month per country.
2. **Data Extraction**: Pull raw market data from Google Sheets (app names, companies, genres, downloads, store revenue).
3. **Monthly Transform**:
   - Parse complex tag columns into separate fields
   - Estimate additional revenue based on country-specific genre/company factors
   - Create monthly DataFrame with enriched data
4. **Append Monthly**: Write transformed data to monthly sheet.
5. **Aggregate**: Combine data from all countries, calculate global totals.
6. **Append Aggregate**: Write aggregated data to summary sheet.

##### Configuration

- Raw sheets: Monthly data.ai_{country}_{YYYY-MM}
- Monthly sheets: Processed data per country
- Agg sheet: Global aggregates
- Configs in \configs/data.ai_sheet.yaml\

##### Scheduling

Monthly on the 5th, processing previous month's data.

### Diagnostic Data Pipeline

Comprehensive user and revenue analytics.

#### DAG

\diagnostic_data_dag.py\: Runs daily, calls \un_spark_job.sh diagnostic_data.py {game_id}\.

#### Script

**diagnostic_data.py**: Processes diagnostic data for games.

##### Functions

- \get_tables(game_id)\: Extracts active, charge, and campaign data from GDS using \DataExtraction\.
- \process_daily(active, charge, campaign)\: 
  - Converts charge revenue to USD using currency mapping
  - Calculates various metrics: gross revenue by user type, retention rates, ARPU, etc.
  - Joins with campaign data
  - Aggregates daily metrics
- \process_monthly(active, charge, campaign)\: Similar to daily but monthly aggregations.
- \execute(game_id)\: Gets data, processes daily and monthly, writes to TSN database.

##### Data Flow

1. **Data Extraction**: Pull active users, charges, campaigns from GDS.
2. **Daily Processing**:
   - Currency conversion for revenue
   - Calculate user acquisition costs, LTV, retention, ARPU
   - Join with campaign spend data
   - Aggregate by date, media source, campaign
3. **Monthly Processing**: Similar aggregations at monthly level.
4. **Database Write**: Insert results into TSN diagnostic tables.

##### Metrics Calculated

- Revenue: grossrev_npu, grossrev_nnpu, etc.
- Retention: grossnru01, grossnru07, etc.
- ARPU: grossrpi01, grossrpi30, etc.
- User metrics: installs, DAU, etc.

##### Scheduling

Daily processing of previous day's data.

### Other Pipelines

Miscellaneous operational and test pipelines.

#### Top Users Pipeline

- **DAG**: \w2_top_users_ggsheet_dag.py\
- **Script**: \w2_top_users.py\ - Extracts top users for FW2 and writes to Google Sheets

#### Email Pipeline

- **DAG**: \cs_email_dag.py\
- **Script**: \cs_email.py\ - Sends diagnostic emails

#### Test Pipelines

- \	est_gspread_dag.py\: Tests Google Sheets integration
- \	est_trino_dag.py\: Tests Trino queries
- \	est_rfc_dag.py\: Tests RFC functionality
- \process_data_test.py\: General data processing tests

#### Utility DAGs

- \check_path_dag.py\: Path validation
- \check_user.py\: User checks
- \demo_dag.py\: Demo workflow
- \	sn_devices_data_dag.py\: TSN devices data processing

## Common Components

- **Data Sources**: Postgres GDS/TSN, Google Sheets
- **Processing**: Apache Spark for big data
- **Output**: Google Sheets updates, database writes
- **Scheduling**: Airflow DAGs with cron schedules
- **Configuration**: YAML configs, credentials in JSON
