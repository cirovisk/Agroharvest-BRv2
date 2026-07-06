# AgroHarvest BR - Modern Agricultural Data Lakehouse

This version of AgroHarvest BR migrates the traditional relational stack to a DuckDB + Apache Parquet lakehouse.

The goal is to integrate critical agribusiness data into a columnar repository with low maintenance overhead and high analytical performance.

## Overview

Integrated sources:

1. RNC / CultivarWeb
2. PAM / IBGE
3. ZARC / MAPA
4. CONAB
5. Agrofit
6. SIPEAGRO
7. SIGEF
8. Open-Meteo
9. Satellite NDVI

## Architecture

The project follows the Lakehouse pattern:

- data is stored as Parquet files
- DuckDB runs analytical queries directly on the files
- the API exposes data access endpoints
- Metabase is used for visualization

## Main characteristics

- lower operational cost
- full warehouse portability
- vectorized queries over columnar data
- support for large-scale analysis

## How to run

```bash
cp .env.example .env
docker-compose run --rm app
docker-compose up api
docker-compose run --rm test
```

## Production

The repository includes a production setup for OCI with:

- Gunicorn + Uvicorn workers
- persistent volumes
- a lean Docker image

## Documentation

- Visual architecture: `docs/ARCHITECTURE_VISUAL.md`
- Database metadata: `docs/DATABASE_METADATA.md`
