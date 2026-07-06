# AgroHarvest BR - Modern Data Lakehouse Agrícola

Esta versão do AgroHarvest BR migra a base relacional tradicional para um Data Lakehouse baseado em DuckDB e Apache Parquet.

O objetivo é integrar dados críticos do agronegócio brasileiro em um repositório colunar com baixa manutenção e alta performance analítica.

## Visão geral

Fontes integradas:

1. RNC / CultivarWeb
2. PAM / IBGE
3. ZARC / MAPA
4. CONAB
5. Agrofit
6. SIPEAGRO
7. SIGEF
8. Open-Meteo
9. NDVI por satélite

## Arquitetura

O projeto segue o padrão Lakehouse:

- os dados são armazenados em arquivos Parquet
- o DuckDB executa consultas analíticas diretamente sobre os arquivos
- a API expõe endpoints para consumo dos dados
- o Metabase é usado para visualização

## Características principais

- redução de custo operacional
- portabilidade total do data warehouse
- consultas vetorizadas em dados colunares
- suporte a análises em grandes volumes

## Como executar

```bash
cp .env.example .env
docker-compose run --rm app
docker-compose up api
docker-compose run --rm test
```

## Produção

O projeto possui configuração de produção para OCI com:

- Gunicorn + Uvicorn workers
- volumes persistentes
- imagem Docker enxuta

## Documentação

- Arquitetura visual: `docs/ARCHITECTURE_VISUAL.md`
- Metadados do banco: `docs/DATABASE_METADATA.md`
