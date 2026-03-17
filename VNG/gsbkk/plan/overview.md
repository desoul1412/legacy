## Description
The GSBKK (GS Big Data Kit Vietnam) currently uses a complex, JSON-layout-driven ETL engine for processing data pipelines. This project aims to simplify this flow by shifting to a **decentralized, SQL-first layered architecture** (Raw -> ETL -> STD -> CONS). 

Control is shifted from a centralized engine to the code itself, allowing developers to manage pipelines through GitLab without requiring Airflow admin permissions.

## Core Problems with Existing Flow
1. **JSON Overhead**: Every pipeline requires a JSON layout, creating a layer of abstraction that makes debugging from the Airflow Web UI difficult.
2. **Monolithic Templates**: SQL templates (Jinja2) are often 400+ lines, making them hard to maintain and prone to error.
3. **Implicit Logic**: The ETL Engine handles variable substitution and path expansion in ways that are opaque when looking at DAG logs.
4. **Bureaucratic Friction**: The current complexity makes it hard for analysts to contribute without deep engine knowledge.

## Proposed Vision: SQL-First Layered Architecture
1. **Simplified API Extraction**: 
   - Extract raw API data (SensorTower, Facebook, etc.) directly to a "Raw" zone in the database/HDFS.
   - Decouple API configuration from execution logic.
2. **Layered Transformation Steps**:
   - **RAW**: Direct ingest from source (JSON/CSV/JDBC).
   - **ETL**: Initial cleaning, normalization, and deduplication.
   - **STD**: Standardization into common schemas (e.g., standardized User Activity).
   - **CONS**: Consolidate data for business use cases (LTV, Forecasts, Dashboards).
3. **SQL Transformation**:
   - Use direct SQL/SparkSQL templates for inter-layer moves.
   - Eliminate JSON layout files as the primary driver.
   - Transformations are defined as SQL files that read from one layer and write to the next.

## Complexity Assessment
- **Size**: ðŸ¡ Medium
- **Complexity Signals**:
  - Legacy Migration: Moving away from the `etl_engine.py` wrapper.
  - Integration: Must still connect to company Airflow.
  - Data Scale: Handles data-heavy tasks like LTV and SensorTower market research.

## Stakeholders
- **Data Engineering**: Maintain the core execution scripts (simplified).
- **Data Analysts**: Write the SQL templates for ETL/STD/CONS steps.
- **Business Users**: Consume the finalized data from CONS layer (Postgres/Tableau).
