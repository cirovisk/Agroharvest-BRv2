# Pipeline: Open-Meteo (Weather)

Extraction and consolidation of historical climate and weather data through a global API with columnar persistence.

## 📌 Data Source
- **Agency:** Open-Meteo.
- **Origin:** [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api).
- **Coverage:** 100% of the 5,570 Brazilian municipalities through global model interpolation.

## 🛠️ Extraction Process
1.  **Geocoding:** Maps latitude/longitude for each DuckDB `id_municipio`.
2.  **Parallelism:** Asynchronous requests through `ThreadPoolExecutor` to maximize network throughput.
3.  **Local Cache:** Temporarily persists coordinates to avoid repeated geocoding calls.

## 💾 Lakehouse Storage
- **Format:** Parquet.
- **Location:** `data/storage/fato_meteorologia/data.parquet`.
- **Differentiator:** DuckDB enables ultra-fast spatial and temporal joins between weather data and production data (PAM), making it possible to identify yield losses caused by drought or frost in milliseconds.

## 🔄 Extracted Indicators
- **Total precipitation (mm)**
- **Temperatures (max, min, average)**
- **Relative humidity** when available

---
*Climate data is fundamental for machine learning models and ZARC risk analysis.*
