from fastapi import APIRouter, Query
from typing import Optional

from api.utils import paginate_query

router = APIRouter(prefix="/zarc", tags=["ZARC - Zoneamento Agrícola"])

@router.get("/indicacoes/stats")
def zarc_indicacoes_stats():
    return {"status": "indisponível", "msg": "Dados de indicações brutas (196M) removidos para simplificação Lakehouse."}

@router.get("/indicacoes")
def listar_indicacoes_zarc():
    return {"items": [], "total": 0, "msg": "Este endpoint foca apenas em riscos climáticos."}

@router.get("/risco")
def listar_risco_zarc(
    codigo_ibge: Optional[str] = Query(None, description="Código IBGE do município"),
    cultura: Optional[str] = Query(None, description="Filtro por cultura"),
    id_solo: Optional[str] = Query(None, description="Tipo de solo (1, 2 ou 3)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500)
):
    sql = """
        SELECT 
            z.periodo_plantio,
            z.tipo_solo,
            z.risco_climatico,
            c.nome_padronizado as cultura,
            m.nome as municipio,
            m.uf
        FROM fato_risco_zarc z
        JOIN dim_cultura c ON z.id_cultura = c.id_cultura
        JOIN dim_municipio m ON z.id_municipio = m.id_municipio
        WHERE 1=1
    """
    if codigo_ibge:
        sql += f" AND m.codigo_ibge = '{codigo_ibge}'"
    if cultura:
        sql += f" AND c.nome_padronizado = '{cultura.lower()}'"
    if id_solo:
        sql += f" AND z.tipo_solo = '{id_solo}'"

    return paginate_query(sql, page, page_size)
