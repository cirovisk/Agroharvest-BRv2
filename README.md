# AgroHarvest BR - DuckDB Lakehouse

Lakehouse version of the AgroHarvest BR project, focused on DuckDB, Apache Parquet, and an analytics API.  
Select a language:

- [Português](./README.pt-BR.md)
- [English](./README.en.md)

## Highlights

- DuckDB as an in-process analytical engine
- Compressed Parquet files for columnar storage
- API for agricultural and climate data queries
- Metabase dashboards

## Stack

Python, DuckDB, Apache Parquet, FastAPI, Uvicorn, SlowAPI, Pandas, NumPy, PyArrow, Docker, and Docker Compose.

## Operational Quickstart

```bash
make setup
make ingest
make validate-lakehouse
make api
```

`make validate-lakehouse` checks DuckDB dimensions, required Parquet fact folders, and writes `data/storage/lakehouse_manifest.json`.

## Logs and Audit

Logs are centralized in `data/logs/app.log` with automatic rotation. Each pipeline execution receives a `run_id` that is included in logs and final reports.

Generated files:

- `data/logs/app.log`: API and pipeline log.
- `data/logs/pipeline_status_*.json`: execution summary.
- `data/logs/pipeline_metrics_*.csv`: source-level metrics.

Use `LOG_FORMAT=json` for structured JSON logs.

## Structure

- `data/storage/`: local lakehouse in Parquet + DuckDB
- `src/`: pipeline, API, and data logic
- `tests/`: unit tests
- `docs/`: architecture and technical documentation
