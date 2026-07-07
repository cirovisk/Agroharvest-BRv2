# Lakehouse Metadata - AgroHarvest BR

This document describes the data structure of **AgroHarvest BR**, which uses a **Lakehouse** architecture based on **DuckDB** for dimensions and metadata, and **Apache Parquet** for high-volume fact tables.

## 🏗️ Data Architecture

The data ecosystem is organized as a **Star Schema**, optimized for analytical queries (OLAP).

- **Dimensions:** Stored in the persistent `data/storage/cultivares.duckdb` file.
- **Facts:** Stored as **Brotli**-compressed Parquet files in the `data/storage/` directory.

---

## 📐 Dimensions (Persisted in DuckDB)

Dimensions are small and are used to guarantee referential integrity and simplify API filters.

### `dim_cultura`
Standardized crop names (soybean, corn, wheat, and so on).
- `id_cultura` (INTEGER): Primary key (sequence).
- `nome_padronizado` (VARCHAR): Unique name in snake_case.

### `dim_municipio`
Brazilian municipality base (IBGE).
- `id_municipio` (INTEGER): Primary key.
- `codigo_ibge` (VARCHAR): Seven-digit code.
- `nome` (VARCHAR): Official name.
- `uf` (VARCHAR): State abbreviation.

---

## 📊 Facts (Stored in Parquet)

Fact tables contain volume indicators. DuckDB reads them as external tables through globbing patterns (`*.parquet`).

### `fato_registro_cultivares` (Fonte: MAPA/SNPC)
- Local: `data/storage/fato_registro_cultivares/data.parquet`
- Campos: `nr_registro`, `id_cultura`, `id_mantenedor`, `cultivar`, `situacao`, `data_reg`.

### `fato_producao_pam` (Fonte: IBGE/SIDRA)
- Local: `data/storage/fato_producao_pam/data.parquet`
- Campos: `id_cultura`, `id_municipio`, `ano`, `area_plantada_ha`, `qtde_produzida_ton`, `valor_producao_mil_reais`.

### `fato_risco_zarc` (Fonte: MAPA/ZARC)
- Local: `data/storage/fato_risco_zarc/data_{cultura}.parquet` (partitioned by crop)
- Campos: `id_cultura`, `id_municipio`, `tipo_solo`, `periodo_plantio`, `risco_climatico`.

### `fato_producao_conab` (Fonte: CONAB)
- Local: `data/storage/fato_producao_conab/data.parquet`
- Campos: `id_cultura`, `uf`, `ano_agricola`, `safra`, `producao_mil_t`, `area_plantada_mil_ha`.

### `fato_meteorologia` (Fonte: Open-Meteo)
- Local: `data/storage/fato_meteorologia/data.parquet`
- Campos: `id_municipio`, `data`, `precipitacao_total_mm`, `temp_media_c`, `temp_max_c`, `temp_min_c`.

### `fato_ndvi_satelite` (Fonte: Sensoriamento Remoto / MODIS)
- Local: `data/storage/fato_ndvi_satelite/data.parquet`
- Campos: `id_municipio`, `ano`, `ndvi_max_safra`, `ndvi_mean_safra`.

---

## ⚙️ Technical Specifications (OLAP Optimization)

1.  **Storage Format:** Apache Parquet (v2.6).
2.  **Compression:** `Brotli` (level 4) for the best balance between compression ratio and decompression speed.
3.  **Query Engine:** [DuckDB](https://duckdb.org/).
    - Queries run through the **Vectored Execution engine**.
    - Native support for **Predicate Pushdown**, reading only the required filters from Parquet.
    - Native support for **Projection Pushdown**, reading only the requested columns.

## 🔄 Freshness and Auditing

Unlike a traditional SQL database, auditing is based on file timestamps in the filesystem. The ingestion pipeline overwrites files on each full load, ensuring the API always serves the freshest available Lakehouse version.

---
*This data dictionary reflects the modernization to the DuckDB + Parquet stack.*
