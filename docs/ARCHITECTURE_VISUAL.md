# AgroHarvest BR - Lakehouse Architecture and Modeling

This document details the data structure and information flow of the AgroHarvest BR project, now operating under the **Data Lakehouse** paradigm.

## 1. Data Model (Star Schema)

Although physical storage is implemented with **Apache Parquet** files, the logical model follows a **Star Schema**. DuckDB acts as the engine that provides a SQL interface over those files, guaranteeing referential integrity in dimensions and analytical performance over facts.

```mermaid
erDiagram
    DIM-CULTURA ||--o{ FATO-CULTIVAR : "has"
    DIM-CULTURA ||--o{ FATO-PAM : "produces"
    DIM-CULTURA ||--o{ FATO-ZARC : "risk"
    DIM-CULTURA ||--o{ FATO-CONAB : "estimate"
    DIM-CULTURA ||--o{ FATO-AGROFIT : "inputs"
    DIM-CULTURA ||--o{ FATO-SIGEF : "seeds"
    
    DIM-MUNICIPIO ||--o{ FATO-PAM : "location"
    DIM-MUNICIPIO ||--o{ FATO-ZARC : "location"
    DIM-MUNICIPIO ||--o{ FATO-METEOROLOGIA : "weather"
    DIM-MUNICIPIO ||--o{ FATO-FERTILIZANTES : "establishments"
    DIM-MUNICIPIO ||--o{ FATO-SIGEF : "location"
    DIM-MUNICIPIO ||--o{ FATO-NDVI : "satellite"
    
    DIM-MANTENEDOR ||--o{ FATO-CULTIVAR : "maintains"

    DIM-CULTURA {
        int id_cultura PK "Persisted in DuckDB"
        string nome_padronizado
    }
    DIM-MUNICIPIO {
        int id_municipio PK "Persisted in DuckDB"
        string codigo_ibge
        string nome
        string uf
    }
    FATO-PAM {
        string parquet_file "data/storage/fato_producao_pam/*.parquet"
        int id_cultura FK
        int id_municipio FK
        int ano
        float area_plantada_ha
        float qtde_produzida_ton
    }
    FATO-NDVI {
        string parquet_file "data/storage/fato_ndvi_satelite/*.parquet"
        int id_municipio FK
        int ano
        float ndvi_max_safra
        float ndvi_mean_safra
    }
```

## 2. Data Flow (Lakehouse Engine)

The pipeline uses the **Registry Pattern** for ingestion and **DuckDB** for the service layer. Data is extracted, cleaned, and saved in columnar format (Parquet) with **Brotli** compression, optimizing I/O and storage cost.

```mermaid
graph LR
    subgraph "External Sources"
        MAPA["MAPA APIs/CSVs"]
        IBGE["IBGE (SIDRA)"]
        OM["Open-Meteo"]
        CONAB["CONAB"]
    end

    subgraph "Pipeline Engine (Python)"
        REG["Registry"]
        SRC["Sources (E+C+L)"]
        DIM["Dimensions (DuckDB)"]
        PQ["Parquet Writer (Brotli)"]
    end

    subgraph "Lakehouse Storage"
        DDB[("cultivares.duckdb")]
        STG[("data/storage/*.parquet")]
    end

    subgraph "Consumption"
        API["FastAPI (SQL)"]
        MB["Metabase"]
    end

    MAPA --> SRC
    IBGE --> SRC
    OM --> SRC
    CONAB --> SRC
    
    SRC --> REG
    REG --> DIM
    DIM --> DDB
    SRC --> PQ
    PQ --> STG
    
    DDB -.-> API
    STG -.-> API
    API --> MB
```

## 3. Benefits of the Current Architecture

1.  **Columnar Storage:** Facts with millions of rows are stored in Parquet, allowing DuckDB to read only the columns required for each query and drastically reduce memory usage.
2.  **Zero-Latency Service:** Because DuckDB runs inside the same process as the API, there is no network latency between the application server and the database.
3.  **Portability (Cloud Native):** The `data/storage` repository can be mounted as a volume in any cloud (OCI, AWS, Azure) without expensive managed database services.
4.  **Horizontal Read Scalability:** Multiple API instances can efficiently read the same Parquet files simultaneously.

---
*Documentation updated for the AgroHarvest BR Lakehouse modernization phase.*
