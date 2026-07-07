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

## Structure

- `data/storage/`: local lakehouse in Parquet + DuckDB
- `src/`: pipeline, API, and data logic
- `tests/`: unit tests
- `docs/`: architecture and technical documentation
