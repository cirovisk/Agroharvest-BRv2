# AgroHarvest BR - Modern Data Lakehouse Agrícola 🦆🌾

## 🏗️ Arquitetura do Sistema: Lakehouse Pattern

Este projeto é uma solução de **Engenharia de Dados** de alto desempenho que migrou de uma arquitetura relacional tradicional (PostgreSQL) para um modelo de **Data Lakehouse** baseado em **DuckDB** e **Apache Parquet**.

A solução consolida dados críticos do agronegócio brasileiro (RNC, PAM, ZARC, CONAB, Agrofit) em um repositório de arquivos colunares otimizados, permitindo análises complexas com custo de infraestrutura zero e performance superior a bancos de dados convencionais.

## 🚀 Por que DuckDB + Parquet? (Diferenciais Técnicos)

Para este projeto de portfólio, optou-se por uma stack moderna de **OLAP (Online Analytical Processing)** que traz benefícios diretos para o ambiente de produção:

1.  **Armazenamento Colunares (Parquet + Brotli):** Redução de até 80% no espaço em disco em comparação ao SQL tradicional, utilizando compressão agressiva e leitura seletiva de colunas.
2.  **Performance Vetorizada:** O DuckDB executa queries SQL diretamente sobre os arquivos Parquet usando execução vetorial, o que o torna ordens de grandeza mais rápido que o Postgres para agregações (`SUM`, `AVG`, `GROUP BY`) em milhões de linhas.
3.  **Custo Operacional Zero:** Por ser um motor *in-process*, não exige a manutenção ou o custo de uma instância de banco de dados ativa (como RDS ou OCI Autonomous), sendo ideal para hospedagem em instâncias *Always Free* ou Cloud Functions.
4.  **Portabilidade Total:** O Data Warehouse inteiro é um diretório de arquivos, facilitando migrações, backups e versionamento de dados.

## 🖥️ Dashboard - Visualização de Dados

![Dashboard AgroHarvest Success](./assets/dashboard.gif)

*Visualização analítica consolidada no Metabase, integrando fluxos de produção (PAM), viabilidade climática (ZARC) e registros genéticos (RNC/SIGEF).*

## 📊 Fontes de Dados Integradas

<details>
<summary>Clique para ver o detalhamento das 8 fontes</summary>

1.  **MAPA/SNPC (CultivarWeb):** Registro Nacional de Cultivares (RNC).
2.  **IBGE/SIDRA (PAM):** Produção Agrícola Municipal (Séries históricas).
3.  **MAPA/ZARC:** Zoneamento Agrícola de Risco Climático (Janelas de plantio por solo/ciclo).
4.  **CONAB:** Produção, produtividade e preços médios (Safra atual e histórica).
5.  **MAPA/Agrofit:** Sistema de Agrotóxicos Fitossanitários (Defensivos e alvos biológicos).
6.  **MAPA/SIPEAGRO:** Registro de estabelecimentos produtores de fertilizantes e corretivos.
7.  **MAPA/SIGEF (Sementes):** Controle e fiscalização da produção de tecnologia genética.
8.  **Open-Meteo (Meteorologia):** API global com indicadores históricos diários para os 5570 municípios.

</details>

## 🛠️ Tecnologias Utilizadas

*   **Linguagem:** Python 3.12+
*   **Engine Analítica:** [DuckDB](https://duckdb.org/) (Motor SQL OLAP ultra-rápido)
*   **Storage:** Apache Parquet (Compressão Brotli)
*   **API:** FastAPI, Uvicorn, SlowAPI (Rate Limiting)
*   **Data Processing:** Pandas, NumPy, PyArrow
*   **Qualidade & Infra:** Pytest, Docker & Docker Compose
*   **BI:** Metabase (Conectado via DuckDB Driver)

## 🏗️ Estrutura do Projeto

O projeto segue o padrão **Lakehouse**, onde os dados são organizados em camadas de armazenamento colunar.

```text
.
├── data/
│   └── storage/        # O Data Lakehouse (Arquivos Parquet + DuckDB File)
├── docker/             # Configurações Docker (Python 3.12-slim)
├── src/                # Código-fonte
│   ├── api/            # Camada de API (Consultas SQL nativas via DuckDB)
│   ├── db/             # Gerenciador de conexão e Views dinâmicas
│   ├── pipeline/       # Pipelines de Ingestão (Extractors e Cleaners)
│   └── main.py         # Orquestrador do pipeline
├── tests/              # Testes unitários usando DuckDB in-memory
└── docker-compose.yml  # Orquestração (app, api, test, metabase)
```

## ⚙️ Como Executar

### 1. Configuração Inicial
```bash
cp .env.example .env
```

### 2. Rodar o Pipeline (Ingestão de Dados)
Este comando baixa os dados das fontes oficiais, processa e salva em arquivos Parquet na pasta `data/storage/`.
```bash
docker-compose run --rm app
```

### 3. Subir a API de Analytics
A API disponibiliza endpoints de raio-x municipal, dossiê de insumos e auditoria de safras.
```bash
docker-compose up api
```
*Acesse `http://localhost:8000/docs` para o Swagger.*

### 4. Executar Testes
```bash
docker-compose run --rm test
```

## 🚀 Deploy em Produção (OCI)

Este projeto está preparado para ser hospedado na **Oracle Cloud (OCI)** utilizando as melhores práticas de produção:

1.  **Servidor de Alta Performance:** Uso de Gunicorn com workers Uvicorn para gerenciar múltiplas requisições simultâneas.
2.  **Persistência de Dados:** Configuração de volumes persistentes para garantir que o Data Lakehouse (Parquet/DuckDB) não seja perdido em reinicializações.
3.  **Segurança:** Código fonte imutável dentro da imagem Docker, mapeando apenas as pastas de dados necessárias.

Para rodar em produção:
```bash
# Sobe a API na porta 80 com configurações de produção
docker-compose -f docker-compose.prod.yml up -d api
```

## ⚖️ Licença e Uso de Dados

- **Código:** Licença [MIT](LICENSE).
- **Dados:** Bases públicas regidas pela Lei de Acesso à Informação (LAI). Fontes: IBGE, CONAB, MAPA e Open-Meteo.

---
*Este projeto demonstra a implementação de um Modern Data Stack para o agronegócio, focado em performance, escalabilidade e otimização de recursos.*
