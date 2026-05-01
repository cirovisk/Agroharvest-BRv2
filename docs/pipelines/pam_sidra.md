# Pipeline: Produção Agrícola Municipal (PAM/SIDRA)

Extração de dados de produção agrícola das lavouras temporárias através da API v3 do IBGE SIDRA com persistência em **Parquet**.

## 📌 Fonte de Dados
- **Agregado:** Tabela 1612 (Produção Agrícola Municipal - Lavouras temporárias)
- **API:** [SIDRA API](https://apisidra.ibge.gov.br/)
- **Granularidade:** Municipal (Nível 6) e Cultura (C81).

## 🛠️ Processo de Extração
1.  **Metadados Dinâmicos:** O pipeline consulta os metadados da Tabela 1612 para buscar os IDs dinâmicos de cada cultura.
2.  **Requests Paralelos:** Para acelerar a ingestão, o pipeline realiza chamadas assíncronas para cada cultura solicitando área plantada, colhida e produção total.
3.  **Deduplicação:** O processo garante que combinações duplicadas de (Município, Cultura, Ano) sejam tratadas antes da persistência.

## 🔄 Transformações (Cleaners)
- **Normalização:** Mapeamento de códigos SIDRA (D2N, V, D1C) para nomes amigáveis.
- **Pivoteamento:** Transformação de variáveis (Linhas API) em colunas de métricas.
- **Data Types:** Garantia de que valores financeiros e produtivos sejam `float64`.

## 💾 Armazenamento Lakehouse
- **Formato:** Apache Parquet.
- **Local:** `data/storage/fato_producao_pam/data.parquet`.
- **Engine de Serviço:** DuckDB (lê o diretório como uma tabela externa).

---
*Escalável para séries históricas de décadas de produção agrícola.*
