# AgroHarvest BR - Arquitetura e Modelagem Lakehouse

Este documento detalha a estrutura de dados e o fluxo de informações do projeto AgroHarvest BR, agora operando sob o paradigma de **Data Lakehouse**.

## 1. Modelo de Dados (Star Schema)

Embora o armazenamento físico seja feito em arquivos **Apache Parquet**, o modelo lógico segue um **Star Schema (Esquema Estrela)**. O DuckDB atua como o motor que provê uma interface SQL sobre esses arquivos, garantindo integridade referencial nas dimensões e performance analítica nos fatos.

```mermaid
erDiagram
    DIM-CULTURA ||--o{ FATO-CULTIVAR : "possui"
    DIM-CULTURA ||--o{ FATO-PAM : "produz"
    DIM-CULTURA ||--o{ FATO-ZARC : "risco"
    DIM-CULTURA ||--o{ FATO-CONAB : "estimativa"
    DIM-CULTURA ||--o{ FATO-AGROFIT : "insumos"
    DIM-CULTURA ||--o{ FATO-SIGEF : "sementes"
    
    DIM-MUNICIPIO ||--o{ FATO-PAM : "localização"
    DIM-MUNICIPIO ||--o{ FATO-ZARC : "localização"
    DIM-MUNICIPIO ||--o{ FATO-METEOROLOGIA : "clima"
    DIM-MUNICIPIO ||--o{ FATO-FERTILIZANTES : "estabelecimentos"
    DIM-MUNICIPIO ||--o{ FATO-SIGEF : "localização"
    
    DIM-MANTENEDOR ||--o{ FATO-CULTIVAR : "mantém"

    DIM-CULTURA {
        int id_cultura PK "Persistido em DuckDB"
        string nome_padronizado
    }
    DIM-MUNICIPIO {
        int id_municipio PK "Persistido em DuckDB"
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
```

## 2. Fluxo de Dados (Lakehouse Engine)

O pipeline utiliza o **Registry Pattern** para ingestão e o **DuckDB** para a camada de serviço. Os dados são extraídos, limpos e salvos em formato colunar (Parquet) com compressão **Brotli**, otimizando o I/O e o custo de storage.

```mermaid
graph LR
    subgraph "Fontes Externas"
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

    subgraph "Consumo"
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

## 3. Benefícios da Arquitetura Atual

1.  **Storage Colunar:** Os fatos (milhões de linhas) são armazenados em Parquet, permitindo que o DuckDB leia apenas as colunas necessárias para cada query, reduzindo drasticamente o uso de memória.
2.  **Zero-Latency Service:** Como o DuckDB roda dentro do mesmo processo da API, não há latência de rede entre o servidor de aplicação e o banco de dados.
3.  **Portabilidade (Cloud Native):** O repositório `data/storage` pode ser montado como um volume em qualquer nuvem (OCI, AWS, Azure) sem necessidade de serviços de banco de dados gerenciados caros.
4.  **Escalabilidade Horizontal de Leitura:** Várias instâncias da API podem ler os mesmos arquivos Parquet simultaneamente de forma eficiente.

---
*Documentação atualizada para a fase de Modernização Lakehouse do AgroHarvest BR.*
