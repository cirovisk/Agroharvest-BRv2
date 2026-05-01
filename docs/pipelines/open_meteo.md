# Pipeline: Open-Meteo (Meteorologia)

Extração e consolidação de dados climáticos e meteorológicos históricos via API global com persistência colunar.

## 📌 Fonte de Dados
- **Agência:** Open-Meteo.
- **Origem:** [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api).
- **Cobertura:** 100% dos 5570 municípios brasileiros (via interpolação de modelos globais).

## 🛠️ Processo de Extração
1.  **Geocodificação:** Mapeamento de Latitude/Longitude para cada `id_municipio` do DuckDB.
2.  **Paralelismo:** Requisições assíncronas via `ThreadPoolExecutor` para maximizar o throughput de rede.
3.  **Cache Local:** Persistência temporária de coordenadas para evitar chamadas de geocodificação repetitivas.

## 💾 Armazenamento Lakehouse
- **Formato:** Parquet.
- **Local:** `data/storage/fato_meteorologia/data.parquet`.
- **Diferencial:** O DuckDB permite joins espaciais e temporais ultra-rápidos entre o clima (Meteorologia) e a produção (PAM), permitindo identificar quebras de safra por seca ou geada em milissegundos.

## 🔄 Indicadores Extraídos
- **Precipitação Total (mm)**
- **Temperaturas (Max, Min, Média)**
- **Umidade Relativa** (Quando disponível)

---
*Dados climáticos fundamentais para modelos de machine learning e análise de risco ZARC.*
