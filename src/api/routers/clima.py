from fastapi import APIRouter
from datetime import date
from typing import Optional

from api.schemas import MetereologiaSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/clima", tags=["Clima"])

@router.get("/", response_model=PaginatedResponse[MetereologiaSchema])
def get_clima(codigo_ibge: Optional[str] = None, data_inicio: Optional[date] = None, data_fim: Optional[date] = None, page: int = 1, page_size: int = 20):
    sql = """
        SELECT 
            f.id_meteo,
            f.data,
            f.precipitacao_total_mm,
            f.temp_media_c,
            f.umidade_media,
            m.nome as municipio_nome,
            m.uf
        FROM fato_meteorologia f
        JOIN dim_municipio m ON f.id_municipio = m.id_municipio
        WHERE 1=1
    """
    
    if codigo_ibge:
        sql += f" AND m.codigo_ibge = '{codigo_ibge}'"
    if data_inicio:
        sql += f" AND f.data >= '{data_inicio.strftime('%Y-%m-%d')}'"
    if data_fim:
        sql += f" AND f.data <= '{data_fim.strftime('%Y-%m-%d')}'"
        
    return paginate_query(sql, page, page_size)
