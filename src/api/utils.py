import math
import numpy as np
from db.duck_manager import duck_db

def paginate_query(sql_base: str, page: int, page_size: int):
    """
    Paginação nativa com DuckDB.
    Recebe a string SQL base (sem LIMIT/OFFSET), calcula o total e retorna a página solicitada.
    """
    # Conta o total de registros (DuckDB faz isso muito rápido com Parquet)
    count_sql = f"SELECT COUNT(*) as total FROM ({sql_base}) as subq"
    total_df = duck_db.execute_query(count_sql)
    total = int(total_df['total'].iloc[0]) if not total_df.empty else 0
    
    # Busca apenas os dados da página
    offset = (page - 1) * page_size
    data_sql = f"{sql_base} LIMIT {page_size} OFFSET {offset}"
    data_df = duck_db.execute_query(data_sql)
    
    # Converte para dict lidando com nulos
    data_df = data_df.replace({np.nan: None})
    items = data_df.to_dict(orient="records")
    
    total_pages = math.ceil(total / page_size) if page_size > 0 else 0
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "items": items
    }
