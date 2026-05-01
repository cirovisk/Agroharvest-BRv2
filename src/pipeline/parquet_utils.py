import os
import pandas as pd
import logging

log = logging.getLogger(__name__)

def save_as_parquet(df: pd.DataFrame, table_name: str, base_path: str = "data/storage", file_suffix: str = ""):
    """
    Salva um DataFrame em formato Parquet usando compressão Brotli.
    Estrutura: base_path/table_name/data{file_suffix}.parquet
    """
    if df.empty:
        log.warning(f"DataFrame vazio para {table_name}. Nada será salvo.")
        return None

    # Garante que o diretório da tabela exista
    table_path = os.path.join(base_path, table_name)
    os.makedirs(table_path, exist_ok=True)

    # Nome do arquivo (poderíamos usar timestamps ou partições aqui no futuro)
    suffix_str = f"_{file_suffix}" if file_suffix else ""
    file_path = os.path.join(table_path, f"data{suffix_str}.parquet")

    try:
        # Salvando com Brotli (exige pyarrow ou fastparquet)
        df.to_parquet(file_path, compression='brotli', index=False)
        log.info(f"Dados salvos com sucesso: {file_path} ({len(df)} linhas)")
        return file_path
    except Exception as e:
        log.error(f"Erro ao salvar Parquet para {table_name}: {e}")
        raise
