from fastapi import APIRouter
from typing import Optional

from api.schemas import ProducaoPAMSchema, ProducaoConabSchema, SigefProducaoSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/producao", tags=["Produção"])

@router.get("/pam", response_model=PaginatedResponse[ProducaoPAMSchema])
def get_pam(cultura: Optional[str] = None, uf: Optional[str] = None, ano: Optional[int] = None, page: int = 1, page_size: int = 20):
    sql = """
        SELECT 
            p.id_producao,
            p.ano,
            p.area_plantada_ha,
            p.area_colhida_ha,
            p.qtde_produzida_ton,
            p.valor_producao_mil_reais,
            c.nome_padronizado as cultura,
            m.nome as municipio_nome,
            m.uf
        FROM fato_producao_pam p
        JOIN dim_cultura c ON p.id_cultura = c.id_cultura
        JOIN dim_municipio m ON p.id_municipio = m.id_municipio
        WHERE 1=1
    """
    if cultura:
        sql += f" AND c.nome_padronizado = '{cultura.lower()}'"
    if uf:
        sql += f" AND m.uf = '{uf.upper()}'"
    if ano:
        sql += f" AND p.ano = {ano}"

    return paginate_query(sql, page, page_size)

@router.get("/conab", response_model=PaginatedResponse[ProducaoConabSchema])
def get_conab(cultura: Optional[str] = None, uf: Optional[str] = None, ano_agricola: Optional[str] = None, page: int = 1, page_size: int = 20):
    sql = """
        SELECT 
            p.id_conab,
            p.uf,
            p.ano_agricola,
            p.safra,
            p.area_plantada_mil_ha,
            p.producao_mil_t,
            p.produtividade_t_ha,
            c.nome_padronizado as cultura
        FROM fato_producao_conab p
        JOIN dim_cultura c ON p.id_cultura = c.id_cultura
        WHERE 1=1
    """
    if cultura:
        sql += f" AND c.nome_padronizado = '{cultura.lower()}'"
    if uf:
        sql += f" AND p.uf = '{uf.upper()}'"
    if ano_agricola:
        sql += f" AND p.ano_agricola = '{ano_agricola}'"

    return paginate_query(sql, page, page_size)

@router.get("/sigef", response_model=PaginatedResponse[SigefProducaoSchema])
def get_sigef(cultura: Optional[str] = None, uf: Optional[str] = None, safra: Optional[str] = None, page: int = 1, page_size: int = 20):
    sql = """
        SELECT 
            s.id_sigef_producao,
            s.safra,
            s.especie,
            s.categoria,
            s.area_ha,
            s.producao_bruta_t,
            s.producao_est_t,
            c.nome_padronizado as cultura,
            m.nome as municipio_nome,
            m.uf
        FROM fato_sigef_producao s
        JOIN dim_cultura c ON s.id_cultura = c.id_cultura
        JOIN dim_municipio m ON s.id_municipio = m.id_municipio
        WHERE 1=1
    """
    if cultura:
        sql += f" AND c.nome_padronizado = '{cultura.lower()}'"
    if uf:
        sql += f" AND m.uf = '{uf.upper()}'"
    if safra:
        sql += f" AND s.safra = '{safra}'"

    return paginate_query(sql, page, page_size)
