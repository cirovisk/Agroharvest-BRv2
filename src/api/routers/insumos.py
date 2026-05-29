from typing import Optional

from fastapi import APIRouter

from api.schemas import AgrofitSchema, FertilizanteSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/insumos", tags=["Insumos"])


@router.get("/agrofit", response_model=PaginatedResponse[AgrofitSchema])
def get_agrofit(cultura: Optional[str] = None, classe: Optional[str] = None, page: int = 1, page_size: int = 20):
    sql = """
        SELECT 
            row_number() over () as id_agrofit,
            a.nr_registro,
            a.marca_comercial,
            a.classe,
            a.praga_comum,
            c.nome_padronizado as cultura
        FROM fato_agrofit a
        JOIN dim_cultura c ON a.id_cultura = c.id_cultura
        WHERE 1=1
    """
    params = []
    if cultura:
        sql += " AND c.nome_padronizado = ?"
        params.append(cultura.lower().strip())
    if classe:
        sql += " AND a.classe = ?"
        params.append(classe.upper().strip())

    return paginate_query(sql, page, page_size, params)


@router.get("/fertilizantes", response_model=PaginatedResponse[FertilizanteSchema])
def get_fertilizantes(uf: Optional[str] = None, atividade: Optional[str] = None, page: int = 1, page_size: int = 20):
    sql = """
        SELECT 
            row_number() over () as id_fertilizante,
            uf,
            municipio,
            nr_registro_estabelecimento,
            razao_social,
            area_atuacao,
            atividade
        FROM fato_fertilizantes_estabelecimentos
        WHERE 1=1
    """
    params = []
    if uf:
        sql += " AND uf = ?"
        params.append(uf.upper().strip())
    if atividade:
        sql += " AND atividade = ?"
        params.append(atividade.upper().strip())

    return paginate_query(sql, page, page_size, params)
