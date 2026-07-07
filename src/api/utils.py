import math

import numpy as np

from db.duck_manager import duck_db


def paginate_query(sql_base: str, page: int, page_size: int, params: tuple | list | dict | None = None):
    """
    Paginação nativa com DuckDB.
    Recebe a string SQL base (sem LIMIT/OFFSET), calcula o total e retorna a página solicitada.
    """
    # Validate bounds and safety limits to avoid DoS and errors
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 10
    elif page_size > 100:
        page_size = 100

    # Count total records (DuckDB does this very quickly with Parquet)
    count_sql = f"SELECT COUNT(*) as total FROM ({sql_base}) as subq"
    total_df = duck_db.execute_query(count_sql, params)
    total = int(total_df["total"].iloc[0]) if not total_df.empty else 0

    # Fetch only the page data
    offset = (page - 1) * page_size
    data_sql = f"{sql_base} LIMIT {page_size} OFFSET {offset}"
    data_df = duck_db.execute_query(data_sql, params)

    # Convert to dict while handling nulls
    data_df = data_df.replace({np.nan: None})
    items = data_df.to_dict(orient="records")

    total_pages = math.ceil(total / page_size) if page_size > 0 else 0

    return {"total": total, "page": page, "page_size": page_size, "total_pages": total_pages, "items": items}
