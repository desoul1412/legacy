# GSBKK Deployment Guide - Tarball Method

## Overview

This guide covers deploying the GSBKK framework to company-provided Airflow using the **tarball method**. No Docker or runner required.

---

## 🏗️ Architecture

```
Company Airflow (/opt/airflow)
    ↓ (Git sync)
This Repo (/opt/airflow/dags/repo/gsbkk)
    ↓ (tarball creation)
HDFS (hdfs://c0s/user/gsbkk-workspace-yc9t6)
    ├── archives/environment.tar.gz     # Python env
    ├── archives/gsbkk-src.tar.gz       # Code
    ├── configs/TSN_POSTGRES.json       # TSN Postgres credentials
    ├── configs/GDS_POSTGRES.json       # GDS Postgres credentials
    ├── configs/GDS_TRINO.json          # Trino credentials
    ├── configs/tsn-data-*.json         # Google Service Account
    └── postgresql-42.7.4.jar           # JDBC driver
```

---

## 📋 Prerequisites

### 1. HDFS Directory Structure

Create base directory:
```bash
hdfs dfs -mkdir -p hdfs://c0s/user/gsbkk-workspace-yc9t6/{archives,configs,sensortower,game_health,rfc}
```

### 2. Python Environment Tarball

**Build locally:**
```bash
# Create virtual environment
python3 -m venv gsbkk-env
source gsbkk-env/bin/activate

# Install dependencies
pip install -r requirements/base.txt

# Create tarball (must be created from parent directory)
cd ..
tar -czf environment.tar.gz gsbkk-env/

# Upload to HDFS
hdfs dfs -put -f environment.tar.gz hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/
```

**Verify:**
```bash
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/environment.tar.gz
# Should show ~200-500MB depending on dependencies
```

### 3. PostgreSQL JDBC Driver

**Download and upload:**
```bash
wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

hdfs dfs -put -f postgresql-42.7.4.jar hdfs://c0s/user/gsbkk-workspace-yc9t6/
```

### 4. Credentials File

**Create separate JSON files for each system:**

**`TSN_POSTGRES.json`:**
```json
{
  "host": "your-tsn-postgres-host",
  "port": 5432,
  "database": "your-database",
  "user": "your-user",
  "password": "your-password"
}
```

**`GDS_POSTGRES.json`:**
```json
{
  "host": "your-gds-postgres-host",
  "port": 5432,
  "database": "your-database",
  "user": "your-user",
  "password": "your-password"
}
```

**`GDS_TRINO.json`:**
```json
{
  "host": "your-trino-host",
  "port": 8080,
  "catalog": "hive",
  "schema": "default",
  "user": "your-user"
}
```

**`tsn-data-0e06f020fc9b.json`** (Google Service Account):
```json
{
  "type": "service_account",
  "project_id": "your-gcp-project",
  "private_key_id": "your-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "your-sa@your-project.iam.gserviceaccount.com",
  "client_id": "12345...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

**Upload to HDFS:**
```bash
# Set restrictive permissions locally
chmod 600 TSN_POSTGRES.json GDS_POSTGRES.json GDS_TRINO.json tsn-data-0e06f020fc9b.json

# Upload each file
hdfs dfs -put -f TSN_POSTGRES.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f GDS_POSTGRES.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f GDS_TRINO.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f tsn-data-0e06f020fc9b.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/

# Verify upload
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
```

**Security Note:**
- Keep local copies secure (chmod 600)
- HDFS files inherit cluster permissions
- Never commit credential files to Git
- Use `.gitignore` to exclude `*.json` credential files

---

## 🚀 Deployment Steps

### Step 1: Push Code to Git

The company Airflow automatically syncs from your repo to `/opt/airflow/dags/repo/gsbkk`.

**Commit and push:**
```bash
git add .
git commit -m "Add GSBKK v2.0 framework"
git push origin main
```

**Wait for sync** (usually 1-5 minutes depending on company settings)

**Verify sync:**
```bash
# On Airflow server (if you have SSH access)
ls -la /opt/airflow/dags/repo/gsbkk/src/

# Should show:
# - core/
# - pipelines/
# - run_pipeline.py
```

### Step 2: Build and Upload Code Tarball

The `run_pipeline.sh` script automatically creates the code tarball, but you can pre-build it:

**Build locally:**
```bash
cd /opt/airflow/dags/repo/gsbkk

tar -czf gsbkk-src.tar.gz \
    --exclude='*.pyc' \
    --exclude='__pycache__' \
    --exclude='.git' \
    --exclude='dist' \
    --exclude='airflow/logs' \
    --exclude='*.md' \
    src/ configs/
```

**Upload to HDFS:**
```bash
hdfs dfs -put -f gsbkk-src.tar.gz hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/
```

**Or let the script build it automatically** - it will check HDFS and build if missing.

### Step 3: Test Pipeline Execution

**Test locally (without Airflow):**
```bash
cd /opt/airflow/dags/repo/gsbkk

# Make executable
chmod +x run_pipeline.sh

# Test market research - all steps
./run_pipeline.sh market_research 2024-12

# Or test individual step (v2.1.0+)
./run_pipeline.sh market_research extract_top_games 2024-12
```

**Expected output (all steps):**
```
========================================================
GSBKK Pipeline Executor - Tarball Method
========================================================
Pipeline Type:     market_research
...
>>> Setting up local Python environment for Driver...
>>> Downloading Python environment from HDFS...
>>> Extracting Python environment...
>>> Driver Python:   /tmp/driver_env_XXXXXX/bin/python
>>> Executor Python: ./environment/bin/python
>>> Downloading credentials from HDFS...
>>> Credentials loaded from: /tmp/gsbkk_credentials_12345.json
>>> Code tarball found in HDFS: hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/gsbkk-src.tar.gz
========================================================
Running Spark Pipeline
========================================================
>>> Spark Submit Command:
spark-submit ...
```

**Expected output (individual step):**
```
========================================================
GSBKK Pipeline Executor - Tarball Method
========================================================
Pipeline Type:     market_research
Pipeline Step:     extract_top_games
Log Date:          2024-12
...
[2024-12-24 10:15:30] Starting Market Research Pipeline - Step: extract_top_games
[2024-12-24 10:15:35] Extracting top games for 2024-12
[2024-12-24 10:15:40] Fetching top games for VN
[2024-12-24 10:16:15] Saved 50 top games for VN
...
========================================================
Pipeline Execution: SUCCESS
========================================================
```

### Step 4: Deploy Airflow DAGs

**DAGs are already in `/dags` folder** - they will be auto-discovered by Airflow.

**Check DAG files:**
```bash
ls -la /opt/airflow/dags/repo/gsbkk/dags/

# Should show:
# - market_research_dag.py (v2.1.0: 7 tasks with dependencies)
# - game_health_dag.py
# - rolling_forecast_dag.py
# - re_std_dag.py
```

**Verify in Airflow UI:**
1. Open Airflow web UI (provided by your company)
2. Check for DAGs:
   - `market_research_sensortower` (v2.1.0: 7 tasks)
   - `game_health_diagnostics`
   - `rolling_forecast_daily`
   - `re_standardization`

3. Enable DAGs by toggling ON

4. **For Market Research DAG (v2.1.0+)**: View graph to see task dependencies
   ```
   extract_top_games ───┬──> extract_metadata ───┐
                        │                         │
   extract_new_games ───┴──> extract_performance ┴──> process_etl -> process_std -> write_postgres
   ```

---

## 🔧 Configuration

### Environment Variables

The `run_pipeline.sh` sets these automatically:

```bash
# Airflow
AIRFLOW_REPO=/opt/airflow/dags/repo/gsbkk
AIRFLOW_HOME=/opt/airflow

# Hadoop
HADOOP_CONF_DIR=/etc/hadoop
HADOOP_CLIENT_OPTS=-Xmx2147483648 -Djava.net.preferIPv4Stack=true

# Spark
SPARK_HOME=/opt/spark
SPARK_CONF_DIR=/etc/spark
PYSPARK_DRIVER_PYTHON=/tmp/driver_env_XXXXXX/bin/python
PYSPARK_PYTHON=./environment/bin/python

# Proxy (if needed)
HTTP_PROXY=http://proxy.dp.vng.vn:3128
HTTPS_PROXY=http://proxy.dp.vng.vn:3128
```

### Pipeline Configurations

**Location:** `configs/pipelines/`

**Available configs:**
- `market_research_sensortower.json` - SensorTower extraction
- `re_std_l2m_roles.json` - L2M roles re-standardization
- `game_health_diagnostics.json` - Game health monitoring

**Add new configs** by creating JSON files following the layout format in [layouts/README.md](../layouts/README.md)

---

## 📊 Running Pipelines

### Via Bash Script (Manual)

```bash
# Market Research - All steps (monthly)
./run_pipeline.sh market_research 2024-12

# Market Research - Individual steps (v2.1.0+)
./run_pipeline.sh market_research extract_top_games 2024-12
./run_pipeline.sh market_research extract_new_games 2024-12
./run_pipeline.sh market_research extract_metadata 2024-12
./run_pipeline.sh market_research extract_performance 2024-12
./run_pipeline.sh market_research process_etl 2024-12
./run_pipeline.sh market_research process_std 2024-12
./run_pipeline.sh market_research write_postgres 2024-12

# Game Health (daily)
./run_pipeline.sh game_health l2m 2024-12-24

# Rolling Forecast - Daily
./run_pipeline.sh rolling_forecast l2m 2024-12-24 daily

# Rolling Forecast - Sheets to DB
./run_pipeline.sh rolling_forecast l2m 2024-12-24 sheets_to_db_daily

# Re-standardization
./run_pipeline.sh re_std configs/pipelines/re_std_l2m_roles.json 2024-12-24
```

### Via Airflow (Scheduled)

**DAGs run automatically** based on schedule:

- `market_research_sensortower`: Monthly (1st at 2 AM) - **7 tasks in v2.1.0**
- `game_health_diagnostics`: Daily (2 AM)
- `rolling_forecast_daily`: Daily (3 AM)
- `re_standardization`: On-demand

**Manual trigger:**
1. Go to Airflow UI
2. Find DAG
3. Click "Trigger DAG" button
4. **For market_research (v2.1.0+)**: Can trigger individual tasks for debugging
4. Enter parameters (if needed)
5. Click "Trigger"

---

## 🐛 Troubleshooting

### Issue 1: Python Environment Not Found

**Error:**
```
hdfs dfs -get: No such file or directory: hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/environment.tar.gz
```

**Solution:**
```bash
# Rebuild and upload environment
python3 -m venv gsbkk-env
source gsbkk-env/bin/activate
pip install -r requirements/base.txt
cd .. && tar -czf environment.tar.gz gsbkk-env/
hdfs dfs -put -f environment.tar.gz hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/
```

### Issue 2: Credentials Not Found

**Error:**
```
WARNING: Could not download credentials from HDFS
```

**Solution:**
```bash
# Upload all credential files
hdfs dfs -put -f TSN_POSTGRES.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f GDS_POSTGRES.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f GDS_TRINO.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
hdfs dfs -put -f tsn-data-0e06f020fc9b.json hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/

# Verify
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/configs/
# Should show all 4 JSON files
```

### Issue 3: Code Tarball Stale

If you updated code but changes aren't reflected:

**Solution:**
```bash
# Force rebuild and upload
cd /opt/airflow/dags/repo/gsbkk
tar -czf gsbkk-src.tar.gz --exclude='*.pyc' --exclude='__pycache__' src/ configs/
hdfs dfs -put -f gsbkk-src.tar.gz hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/

# Or delete from HDFS to force auto-rebuild
hdfs dfs -rm hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/gsbkk-src.tar.gz
```

### Issue 4: PostgreSQL Driver Not Found

**Error:**
```
java.lang.ClassNotFoundException: org.postgresql.Driver
```

**Solution:**
```bash
# Download driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Upload to HDFS
hdfs dfs -put -f postgresql-42.7.4.jar hdfs://c0s/user/gsbkk-workspace-yc9t6/

# Verify
hdfs dfs -ls hdfs://c0s/user/gsbkk-workspace-yc9t6/postgresql-42.7.4.jar
```

### Issue 5: Spark Submit Fails

**Check logs:**
```bash
# Yarn application logs
yarn logs -applicationId <application_id>

# Airflow task logs
# Go to Airflow UI → DAGs → Task Instance → View Logs
```

**Common fixes:**
- Increase memory: Edit `run_pipeline.sh` → `--driver-memory` / `--executor-memory`
- Check HDFS paths: Verify all paths exist
- Verify credentials: Test DB connections manually

---

## 📁 Directory Structure in HDFS

```
hdfs://c0s/user/gsbkk-workspace-yc9t6/
├── archives/
│   ├── environment.tar.gz          # Python environment (~300MB)
│   └── gsbkk-src.tar.gz            # Code tarball (~10MB)
├── configs/
│   ├── TSN_POSTGRES.json           # TSN Postgres credentials
│   ├── GDS_POSTGRES.json           # GDS Postgres credentials
│   ├── GDS_TRINO.json              # Trino credentials
│   └── tsn-data-0e06f020fc9b.json  # Google Service Account
├── postgresql-42.7.4.jar           # JDBC driver (~1MB)
├── sensortower/
│   ├── raw/                        # Raw API responses
│   ├── etl/                        # Processed data
│   └── std/                        # Standardized data
├── game_health/
│   └── <game_id>/
│       ├── diagnostic/
│       ├── server_performance/
│       └── packages_performance/
├── rfc/
│   └── <game_id>/
│       ├── daily/
│       └── monthly/
└── <game_id>/
    ├── std/                        # Re-standardized data
    └── cons/                       # Consolidated data
```

---

## 🔄 Update Process

### Code Updates

1. **Modify code locally**
2. **Test locally:**
   ```bash
   ./run_pipeline.sh <pipeline> <args>
   ```
3. **Commit and push:**
   ```bash
   git add .
   git commit -m "Update: ..."
   git push origin main
   ```
4. **Wait for Airflow sync** (1-5 minutes)
5. **Force tarball rebuild:**
   ```bash
   hdfs dfs -rm hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/gsbkk-src.tar.gz
   ```
6. **Test in Airflow** (trigger DAG manually)

### Configuration Updates

1. **Modify JSON config** in `configs/pipelines/`
2. **Commit and push** (configs are in code tarball)
3. **Rebuild tarball** (or let script auto-rebuild)

### Dependency Updates

1. **Update `requirements/base.txt`**
2. **Rebuild environment tarball:**
   ```bash
   python3 -m venv gsbkk-env-new
   source gsbkk-env-new/bin/activate
   pip install -r requirements/base.txt
   cd .. && tar -czf environment-new.tar.gz gsbkk-env-new/
   hdfs dfs -put -f environment-new.tar.gz hdfs://c0s/user/gsbkk-workspace-yc9t6/archives/environment.tar.gz
   ```
3. **Test with new environment**

---

## 📚 Additional Resources

- **SensorTower API:** [SENSORTOWER_QUICK_REFERENCE.md](SENSORTOWER_QUICK_REFERENCE.md)
- **Layout Format:** [../layouts/README.md](../layouts/README.md)
- **Main Documentation:** [../README.md](../README.md)
- **Jinja2 Templates:** [../JINJA2_GUIDE.md](../JINJA2_GUIDE.md)

---

## ✅ Deployment Checklist

Before deploying to production:

- [ ] Python environment tarball uploaded to HDFS
- [ ] PostgreSQL JDBC driver uploaded to HDFS
- [ ] Credentials file uploaded to HDFS with 600 permissions
- [ ] Code pushed to Git repo
- [ ] Code tarball built and uploaded (or auto-builds on first run)
- [ ] HDFS directory structure created
- [ ] Tested pipeline execution locally
- [ ] DAGs visible in Airflow UI
- [ ] DAGs enabled and scheduled correctly
- [ ] Test run successful in Airflow
- [ ] Monitoring/alerting configured (email on failure)

---

## 🎯 Production Readiness

**This framework is production-ready when:**

✅ All checklist items completed  
✅ Pipelines run successfully in Airflow  
✅ Data appears in target locations (HDFS + Postgres)  
✅ Error handling tested (retry logic, failure notifications)  
✅ Performance acceptable (execution time, resource usage)  
✅ Documentation reviewed by team  

**Monitor for first 1-2 weeks:**
- Airflow DAG run history
- HDFS disk usage
- Postgres table sizes
- Error logs in Yarn applications
