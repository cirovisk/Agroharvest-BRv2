import duckdb
import os
import logging
import pandas as pd

log = logging.getLogger(__name__)

class DuckManager:
    """
    Gerenciador DuckDB para arquitetura Lakehouse.
    Mapeia arquivos Parquet em views SQL automaticamente e guarda dimensões.
    """
    def __init__(self, storage_path="data/storage", db_file="data/storage/cultivares.duckdb"):
        self.storage_path = storage_path
        os.makedirs(self.storage_path, exist_ok=True)
        self.conn = duckdb.connect(database=db_file)
        self.refresh_views()

    def refresh_views(self):
        """
        Varre o diretório de storage e cria/atualiza as views no DuckDB.
        Isso permite consultar os Parquets como tabelas SQL reais.
        """
        if not os.path.exists(self.storage_path):
            log.warning(f"Diretório de storage não encontrado: {self.storage_path}")
            return

        # Lista subpastas que representam nossas "tabelas"
        tables = [d for d in os.listdir(self.storage_path) if os.path.isdir(os.path.join(self.storage_path, d))]
        
        for table in tables:
            # Padrão para ler todos os parquets da pasta da tabela
            parquet_pattern = os.path.join(self.storage_path, table, "*.parquet")
            
            # Verifica se há arquivos antes de criar a view
            files = [f for f in os.listdir(os.path.join(self.storage_path, table)) if f.endswith('.parquet')]
            if not files:
                continue

            try:
                # O DuckDB lê Parquets de forma extremamente eficiente
                self.conn.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{parquet_pattern}')")
                log.info(f"DuckDB View: '{table}' sincronizada com Parquets em {table}/")
            except Exception as e:
                log.error(f"Falha ao mapear tabela {table}: {e}")

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Executa SQL e retorna o resultado como um DataFrame do Pandas."""
        try:
            return self.conn.execute(sql).df()
        except Exception as e:
            log.error(f"Erro na query DuckDB: {e}")
            return pd.DataFrame()

# Instância única (Singleton simplificado para o app)
duck_db = DuckManager()
