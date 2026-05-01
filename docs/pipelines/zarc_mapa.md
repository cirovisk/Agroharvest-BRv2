# Pipeline: Zoneamento Agrícola de Risco Climático (ZARC)

Extração e processamento massivo de cenários de risco climático e recomendações de cultivares utilizando **Modern Data Lakehouse**.

## 📌 Fonte de Dados
- **Agência:** MAPA
- **Origem:** [Portal de Dados Abertos do MAPA](https://dados.agricultura.gov.br/)
- **Método de Acesso:** Arquivos CSV massivos (multi-gigabytes) processados via stream.

## 🛠️ Processo de Ingestão (Otimizado)
Devido ao tamanho extremo dos arquivos, o pipeline utiliza uma estratégia de **Streaming para Parquet**:
1.  **Download Unificado:** O pipeline baixa a base consolidada do portal de dados abertos.
2.  **Split por Cultura:** O arquivo gigante é quebrado em fragmentos menores por cultura alvo (Soja, Milho, etc).
3.  **Conversão Colunar:** Os dados são transformados e salvos em formato **Apache Parquet** com compressão **Brotli**.
4.  **Memória Eficiente:** O processamento é feito em chunks, mantendo o uso de RAM constante mesmo para arquivos de 2GB+.

## 🔄 Performance Lakehouse (DuckDB)
Substituímos os índices B-Tree do PostgreSQL pela engine analítica do **DuckDB**:
- **Projection Pushdown:** Ao consultar o risco climático de um município, o DuckDB lê apenas as colunas necessárias do arquivo Parquet.
- **Partition Discovery:** A API mapeia os arquivos automaticamente. Consultas por cultura são resolvidas acessando apenas os arquivos `data_{cultura}.parquet`, eliminando a necessidade de scan em toda a base.

## 💾 Armazenamento
- **Fato:** `data/storage/fato_risco_zarc/`.
- **Formato:** Parquet (Brotli compressed).

## 📥 Guia de Expansão
Atualmente, o projeto processa nativamente **Soja, Milho, Trigo, Algodão e Cana-de-Açúcar**. Para adicionar uma nova cultura:
1.  Certifique-se que o arquivo bruto está na pasta de download.
2.  Adicione o nome da cultura na lista `TARGET_CROPS` em `src/pipeline/sources/zarc.py`.
3.  Execute o pipeline: `docker-compose run --rm app`.

---
*Otimizado para alto throughput e baixo custo de storage.*
