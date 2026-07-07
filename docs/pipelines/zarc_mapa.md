# Pipeline: Agricultural Climate Risk Zoning (ZARC)

Large-scale extraction and processing of climate risk scenarios and cultivar recommendations using a **Modern Data Lakehouse**.

## 📌 Data Source
- **Agency:** MAPA
- **Origin:** [MAPA Open Data Portal](https://dados.agricultura.gov.br/)
- **Access Method:** Massive multi-gigabyte CSV files processed through streaming.

## 🛠️ Ingestion Process (Optimized)
Because the files are extremely large, the pipeline uses a **streaming-to-Parquet** strategy:
1.  **Unified Download:** The pipeline downloads the consolidated dataset from the open data portal.
2.  **Split by Crop:** The huge file is split into smaller fragments by target crop (soybean, corn, and so on).
3.  **Columnar Conversion:** The data is transformed and saved in **Apache Parquet** format with **Brotli** compression.
4.  **Memory Efficiency:** Processing runs in chunks, keeping RAM usage constant even for files larger than 2 GB.

## 🔄 Lakehouse Performance (DuckDB)
PostgreSQL B-Tree indexes were replaced by the **DuckDB** analytical engine:
- **Projection Pushdown:** When querying climate risk for a municipality, DuckDB reads only the required columns from the Parquet file.
- **Partition Discovery:** The API maps files automatically. Crop queries are resolved by accessing only the `data_{crop}.parquet` files, eliminating the need to scan the whole dataset.

## 💾 Storage
- **Fact:** `data/storage/fato_risco_zarc/`.
- **Format:** Parquet with Brotli compression.

## 📥 Expansion Guide
The project currently processes **soybean, corn, wheat, cotton, and sugarcane** natively. To add a new crop:
1.  Make sure the raw file is in the download folder.
2.  Add the crop name to the `TARGET_CROPS` list in `src/pipeline/sources/zarc.py`.
3.  Run the pipeline: `docker-compose run --rm app`.

---
*Optimized for high throughput and low storage cost.*
