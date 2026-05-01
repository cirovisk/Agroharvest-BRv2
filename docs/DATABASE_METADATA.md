# Metadados do Lakehouse - AgroHarvest BR

Este documento descreve a estrutura de dados do projeto **AgroHarvest BR**, que utiliza uma arquitetura **Lakehouse** baseada em **DuckDB** (para dimensões e metadados) e **Apache Parquet** (para tabelas fato de alta volumetria).

## 🏗️ Arquitetura de Dados

O ecossistema de dados é organizado em um modelo **Star Schema** (Modelo Estrela), otimizado para consultas analíticas (OLAP).

- **Dimensões:** Armazenadas no arquivo persistente `data/storage/cultivares.duckdb`.
- **Fatos:** Armazenadas como arquivos Parquet comprimidos com **Brotli** no diretório `data/storage/`.

---

## 📐 Dimensões (Persistidas em DuckDB)

As dimensões são pequenas e servem para garantir a integridade referencial e facilitar filtros na API.

### `dim_cultura`
Padronização de nomes de culturas (soja, milho, trigo, etc).
- `id_cultura` (INTEGER): Chave primária (Sequence).
- `nome_padronizado` (VARCHAR): Nome único em snake_case.

### `dim_municipio`
Base de municípios brasileiros (IBGE).
- `id_municipio` (INTEGER): Chave primária.
- `codigo_ibge` (VARCHAR): Código de 7 dígitos.
- `nome` (VARCHAR): Nome oficial.
- `uf` (VARCHAR): Sigla do estado.

---

## 📊 Fatos (Armazenadas em Parquet)

As tabelas fato contêm os indicadores volumétricos. Elas são lidas pelo DuckDB como `External Tables` via padrão globbing (`*.parquet`).

### `fato_registro_cultivares` (Fonte: MAPA/SNPC)
- Local: `data/storage/fato_registro_cultivares/data.parquet`
- Campos: `nr_registro`, `id_cultura`, `id_mantenedor`, `cultivar`, `situacao`, `data_reg`.

### `fato_producao_pam` (Fonte: IBGE/SIDRA)
- Local: `data/storage/fato_producao_pam/data.parquet`
- Campos: `id_cultura`, `id_municipio`, `ano`, `area_plantada_ha`, `qtde_produzida_ton`, `valor_producao_mil_reais`.

### `fato_risco_zarc` (Fonte: MAPA/ZARC)
- Local: `data/storage/fato_risco_zarc/data_{cultura}.parquet` (Particionado por cultura)
- Campos: `id_cultura`, `id_municipio`, `tipo_solo`, `periodo_plantio`, `risco_climatico`.

### `fato_producao_conab` (Fonte: CONAB)
- Local: `data/storage/fato_producao_conab/data.parquet`
- Campos: `id_cultura`, `uf`, `ano_agricola`, `safra`, `producao_mil_t`, `area_plantada_mil_ha`.

### `fato_meteorologia` (Fonte: Open-Meteo)
- Local: `data/storage/fato_meteorologia/data.parquet`
- Campos: `id_municipio`, `data`, `precipitacao_total_mm`, `temp_media_c`, `temp_max_c`, `temp_min_c`.

---

## ⚙️ Especificações Técnicas (OLAP Optimization)

1.  **Formato de Armazenamento:** Apache Parquet (v2.6).
2.  **Compressão:** `Brotli` (Nível 4) para o melhor balanço entre taxa de compressão e velocidade de descompressão.
3.  **Engine de Query:** [DuckDB](https://duckdb.org/).
    - Consultas são executadas via **Vectored Execution engine**.
    - Suporte nativo a **Predicate Pushdown** (lê apenas os filtros necessários do Parquet).
    - Suporte a **Projection Pushdown** (lê apenas as colunas solicitadas).

## 🔄 Freshness e Auditoria

Diferente de um banco SQL tradicional, a auditoria é feita pelo timestamp dos arquivos no sistema de arquivos. O pipeline de ingestão sobrescreve os arquivos em cada carga completa, garantindo que a API sempre sirva a "versão mais fresca" disponível no Lakehouse.

---
*Este dicionário de dados reflete a modernização para a stack DuckDB + Parquet.*
