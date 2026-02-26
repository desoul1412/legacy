# FAQ

## Common Issues

**Q: How do I run a specific DAG?**

A: Use the Airflow web UI or CLI:
```bash
airflow dags trigger dag_name
```

**Q: Where are the logs stored?**

A: Logs are typically in `/opt/airflow/logs/` or configured log directory.

**Q: How to add a new data source?**

A: Add configuration in `configs/` and create corresponding DAG/script.

## Troubleshooting

- Check Airflow logs for errors
- Verify configurations in `configs/`
- Ensure dependencies are installed