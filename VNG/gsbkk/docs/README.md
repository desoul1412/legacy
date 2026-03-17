# GSBKK Documentation Index

## Start Here

| What You Need | Document |
|---------------|----------|
| **New to GSBKK?** Start here | [ONBOARDING.md](ONBOARDING.md) |
| **Adding a new pipeline or game** | [ONBOARDING.md — Section 8](ONBOARDING.md#8-adding-a-new-pipeline) |
| **The 5 pipeline patterns** | [ONBOARDING.md — Section 3](ONBOARDING.md#3-the-five-pipeline-patterns) |
| **Debugging a failing task** | [ONBOARDING.md — Section 10](ONBOARDING.md#10-debugging) |
| **First-time HDFS/deployment setup** | [ONBOARDING.md — Section 9](ONBOARDING.md#9-deployment) |
| **SensorTower API reference** | [SENSORTOWER_QUICK_REFERENCE.md](SENSORTOWER_QUICK_REFERENCE.md) |
| **Adding a new game to re-standardization** | [WIKI_RESTANDARDIZATION_GUIDE.md](WIKI_RESTANDARDIZATION_GUIDE.md) |
| **SQL template macros and helpers** | [WIKI_SQL_TEMPLATE_GUIDE.md](WIKI_SQL_TEMPLATE_GUIDE.md) |
| **Google Sheets integration** | [GOOGLE_SHEETS_INTEGRATION.md](GOOGLE_SHEETS_INTEGRATION.md) |

## Documentation Files

```
docs/
├── README.md                         — This index
├── ONBOARDING.md                     — New analyst guide (architecture, patterns, DAG writing, debugging)
├── DEPLOYMENT.md                     — One-time HDFS setup and tarball deployment details
├── WIKI_SQL_TEMPLATE_GUIDE.md        — Jinja2 SQL template reference and macros
├── WIKI_RESTANDARDIZATION_GUIDE.md   — Step-by-step: add a new game to the re-std pipeline
├── GOOGLE_SHEETS_INTEGRATION.md      — GSheet RAW ingestion and efflux
├── SENSORTOWER_QUICK_REFERENCE.md    — SensorTower API endpoints and parameters
└── PROJECTS_GUIDE.md                 — Legacy project reference (partially outdated)
```

> **Note:** `PROJECTS_GUIDE.md` and `DEPLOYMENT.md` contain some references to the old
> JSON layout engine (`run_etl_process.sh`, `layouts/`). The current approach is described
> in `ONBOARDING.md`. The new transform layer is documented in `transform/README.md`.
