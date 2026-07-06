# AgroHarvest BR - DuckDB Lakehouse

Versão Lakehouse do projeto AgroHarvest BR, com foco em DuckDB, Apache Parquet e API de analytics.  
Selecione o idioma:

- [Português](./README.pt-BR.md)
- [English](./README.en.md)

## Destaques

- DuckDB como engine analítica in-process
- Arquivos Parquet comprimidos para armazenamento colunar
- API para consulta de dados agrícolas e climáticos
- Dashboards no Metabase

## Stack

Python, DuckDB, Apache Parquet, FastAPI, Uvicorn, SlowAPI, Pandas, NumPy, PyArrow, Docker e Docker Compose.

## Estrutura

- `data/storage/`: lakehouse local em Parquet + DuckDB
- `src/`: pipeline, API e lógica de dados
- `tests/`: testes unitários
- `docs/`: arquitetura e documentação técnica
