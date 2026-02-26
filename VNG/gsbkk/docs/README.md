# GSBKK Documentation - Where to Find Everything 📚

> **Super Simple:** This page tells you which document to read for what you need.

---

## 🎯 I Want To...

| What You Want | Which Document | Why |
|---------------|----------------|-----|
| **Understand what this project does** | [../README.md](../README.md) | Explains everything in plain English |
| **Run a quick example** | [../QUICKSTART.md](../QUICKSTART.md) | Copy-paste commands that work |
| **Create layout files** | [WIKI_LAYOUT_GUIDE.md](WIKI_LAYOUT_GUIDE.md) | Complete guide to layouts, inputs, outputs, parameters |
| **Use SQL templates & macros** | [WIKI_SQL_TEMPLATE_GUIDE.md](WIKI_SQL_TEMPLATE_GUIDE.md) | Jinja2 templates, macros, variables, conditionals |
| **Add new game to re-standardization** | [WIKI_RESTANDARDIZATION_GUIDE.md](WIKI_RESTANDARDIZATION_GUIDE.md) | Step-by-step tutorial with examples |
| **See all 4 projects** | [PROJECTS_GUIDE.md](PROJECTS_GUIDE.md) | What each project does + when it runs |
| **Advanced Jinja2 reference** | [../JINJA2_GUIDE.md](../JINJA2_GUIDE.md) | Deep dive into template engine |

---

## 📊 The 4 Projects (Detailed Guides)

### 1. Game Health Check (Daily Metrics)
**[layouts/game_health_check/README.md](../layouts/game_health_check/README.md)**

What: Track active players, revenue, VIP levels  
When: Daily at 9 AM  
Who Uses: Business team for daily reports

### 2. Rolling Forecast (Revenue Predictions)
**[layouts/rolling_forecast/README.md](../layouts/rolling_forecast/README.md)**

What: Predict daily revenue → Google Sheets  
When: Daily at 9:20 AM (after health check)  
Who Uses: Finance team for planning

### 3. Re-Standardization (Data Migration)
**[layouts/re-standardization/README.md](../layouts/re-standardization/README.md)**

What: Move old game data to new format  
When: Daily for legacy games  
Who Uses: Data engineering team

### 4. SensorTower (Market Research)
**[layouts/sensortower/README.md](../layouts/sensortower/README.md)**

What: Extract competitor data from SensorTower API  
When: Monthly on 1st  
Who Uses: Market research team

---

## 📖 Wiki Documentation (Complete Guides)

### Core Documentation

| Guide | Purpose | Audience |
|-------|---------|----------|
| [WIKI_LAYOUT_GUIDE.md](WIKI_LAYOUT_GUIDE.md) | How to create and configure layout files | All developers |
| [WIKI_SQL_TEMPLATE_GUIDE.md](WIKI_SQL_TEMPLATE_GUIDE.md) | SQL templates with Jinja2 macros | SQL developers |
| [WIKI_RESTANDARDIZATION_GUIDE.md](WIKI_RESTANDARDIZATION_GUIDE.md) | Adding new games step-by-step | Data engineers |

### Operations & Deployment

| Guide | Purpose | Audience |
|-------|---------|----------|
| [DEPLOYMENT.md](DEPLOYMENT.md) | Production deployment to Airflow | DevOps, Data engineers |
| [AIRFLOW_DEPLOYMENT_NOTES.md](AIRFLOW_DEPLOYMENT_NOTES.md) | Troubleshooting & specific fixes | Operations team |

**These wiki guides replace older documentation and provide comprehensive, up-to-date information.**

---

## 🆕 Jinja2 Templates (Advanced)

| Document | What It's For |
|----------|---------------|
| **[JINJA2_GUIDE.md](../JINJA2_GUIDE.md)** | Complete guide with examples |
| **[JINJA2_QUICKSTART.md](../JINJA2_QUICKSTART.md)** | Quick reference cheat sheet |
| **[templates/README.md](../templates/README.md)** | All template files explained |

**What are templates?** Like Mad Libs for SQL:
```sql
-- Instead of writing 12 separate SQL files (one per game):
SELECT * FROM {gameId}_revenue WHERE date = '{logDate}'

-- Template fills in: gameId=fw2, logDate=2025-12-26
-- Result: SELECT * FROM fw2_revenue WHERE date = '2025-12-26'
```

---

## 🔧 Technical References (For Developers)

| Document | What It Explains |
|----------|------------------|
| **[layouts/README.md](../layouts/README.md)** | How layout JSON files work |
| **[SENSORTOWER_QUICK_REFERENCE.md](SENSORTOWER_QUICK_REFERENCE.md)** | SensorTower API usage |
| **[DEPLOYMENT.md](DEPLOYMENT.md)** | How to deploy to servers |
| **[dags/README.md](../dags/README.md)** | Airflow DAG organization |

---

## 📖 Documentation Structure

```
gsbkk/
├── README.md                          # 👈 Project overview (START HERE)
├── QUICKSTART.md                      # Quick examples
│
├── JINJA2_GUIDE.md                    # 👈 Complete Jinja2 guide (templates)
├── JINJA2_QUICKSTART.md               # Quick reference
│
├── docs/                              # Technical docs
│   ├── README.md                      # This file (index)
│   ├── PROJECTS_GUIDE.md              # 👈 Complete projects guide
│   ├── DEPLOYMENT.md                  # Deployment guide
│   └── SENSORTOWER_QUICK_REFERENCE.md # API reference
│
├── dags/
│   └── README.md                      # Airflow DAG organization
│
├── layouts/
│   ├── README.md                      # Layout format specification
│   ├── game_health_check/
│   │   └── README.md                  # Game health project docs
│   ├── rolling_forecast/
│   │   └── README.md                  # Rolling forecast docs
│   └── sensortower/
│       └── README.md                  # SensorTower project docs
│
└── templates/
    └── README.md                      # Template file reference
```

---

## 🎯 Quick Navigation

### I want to...

**Understand the project**
→ Read [README.md](../README.md)

**Run examples quickly**
→ See [QUICKSTART.md](../QUICKSTART.md)

**Use Jinja2 templates**
→ Start with [JINJA2_GUIDE.md](../JINJA2_GUIDE.md)
Understand all projects**
→ Read [PROJECTS_GUIDE.md](PROJECTS_GUIDE.md)

**Add a new game to re-standardization**
→ Follow [layouts/re-standardization/README.md](../layouts/re-standardization/README.md)

**
**Create a layout file**
→ See [layouts/README.md](../layouts/README.md)

**Deploy to production**
→ Follow [DEPLOYMENT.md](DEPLOYMENT.md)

**Use SensorTower API**
→ Check [SENSORTOWER_QUICK_REFERENCE.md](SENSORTOWER_QUICK_REFERENCE.md)

**Work on a specific project**
→ See project READMEs in `layouts/*/README.md`

---

## 📝 Documentation Standards

All documentation follows these principles:
- ✅ **Practical examples** - Show, don't just tell
- ✅ **Quick start sections** - Get running in minutes
- ✅ **Clear structure** - TOC, headings, code blocks
- ✅ **Up to date** - Reflects current codebase

---

**Last Updated:** December 26, 2025
