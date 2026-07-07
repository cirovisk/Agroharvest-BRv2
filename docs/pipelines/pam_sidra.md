# Pipeline: Municipal Agricultural Production (PAM/SIDRA)

Extraction of agricultural production data for temporary crops through the IBGE SIDRA v3 API with **Parquet** persistence.

## 📌 Data Source
- **Aggregate:** Table 1612 (Municipal Agricultural Production - temporary crops)
- **API:** [SIDRA API](https://apisidra.ibge.gov.br/)
- **Granularity:** Municipality (level 6) and crop (C81).

## 🛠️ Extraction Process
1.  **Dynamic Metadata:** The pipeline queries the metadata for Table 1612 to retrieve the dynamic IDs for each crop.
2.  **Parallel Requests:** To speed up ingestion, the pipeline performs asynchronous calls for each crop requesting planted area, harvested area, and total production.
3.  **Deduplication:** The process ensures that duplicate combinations of (municipality, crop, year) are handled before persistence.

## 🔄 Transformations (Cleaners)
- **Normalization:** Maps SIDRA codes (D2N, V, D1C) to friendly names.
- **Pivoting:** Transforms variables from API rows into metric columns.
- **Data Types:** Ensures that financial and production values are `float64`.

## 💾 Lakehouse Storage
- **Format:** Apache Parquet.
- **Location:** `data/storage/fato_producao_pam/data.parquet`.
- **Service Engine:** DuckDB, which reads the directory as an external table.

---
*Scalable for historical series spanning decades of agricultural production.*
