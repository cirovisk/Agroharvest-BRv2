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

Fluxo recomendado:

```bash
make setup
make ingest
make validate-lakehouse
make api
```

`make validate-lakehouse` verifica dimensoes DuckDB, arquivos Parquet obrigatorios e gera um manifesto em `data/storage/lakehouse_manifest.json`.

## Logs e auditoria

O projeto grava logs centralizados em `data/logs/app.log` com rotacao automatica. Cada execucao do pipeline recebe um `run_id`, registrado nos logs e no relatorio final.

Arquivos gerados:

- `data/logs/app.log`: log da API e do pipeline.
- `data/logs/pipeline_status_*.json`: resumo da execucao.
- `data/logs/pipeline_metrics_*.csv`: metricas por fonte.

Para logs estruturados em JSON, use:

```env
LOG_FORMAT=json
```

## Produção

O projeto possui configuração de produção para OCI com:

- Gunicorn + Uvicorn workers
- volumes persistentes
- imagem Docker enxuta

## Documentação

- Arquitetura visual: `docs/ARCHITECTURE_VISUAL.md`
- Metadados do banco: `docs/DATABASE_METADATA.md`
