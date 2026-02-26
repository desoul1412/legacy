# Architecture

## Overview

The GSBKK project is a data pipeline built with Apache Airflow and Spark.

## Components

- **DAGs**: Airflow Directed Acyclic Graphs in `dags/`
- **Scripts**: Python scripts in `dags/scripts/`
- **Configurations**: YAML and JSON configs in `configs/`
- **SQL**: Query files in `dags/sql/`

## Data Flow

1. Data ingestion from various sources
2. Processing with Spark jobs
3. Storage in data warehouse
4. Reporting and analytics