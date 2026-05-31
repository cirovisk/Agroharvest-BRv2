from typing import Optional

from fastapi import APIRouter, Query

from api.schemas import PaginatedResponse, ProducaoConabSchema, ProducaoPAMSchema, SigefProducaoSchema
from api.utils import paginate_query

router = APIRouter(prefix="/producao", tags=["Produção"])


@router.get("/pam", response_model=PaginatedResponse[ProducaoPAMSchema])
def get_pam(
    cultura: Optional[str] = None,
    uf: Optional[str] = None,
    ano: Optional[int] = None,
    page: int = Query(1, ge=1, description="Número da página"),
    page_size: int = Query(20, ge=1, le=100, description="Itens por página (máximo: 100)"),
) -> PaginatedResponse:
    """
    Obtém dados de produção agrícola municipal (PAM) do IBGE a partir de views Parquet paginadas.
    Permite filtros opcionais por cultura, unidade federativa (UF) e ano.
    """
    sql = """
        SELECT 
            row_number() over () as id_producao,
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
    params = []
    if cultura:
        sql += " AND c.nome_padronizado = ?"
        params.append(cultura.lower().strip())
    if uf:
        sql += " AND m.uf = ?"
        params.append(uf.upper().strip())
    if ano:
        sql += " AND p.ano = ?"
        params.append(ano)

    return paginate_query(sql, page, page_size, params)


@router.get("/conab", response_model=PaginatedResponse[ProducaoConabSchema])
def get_conab(
    cultura: Optional[str] = None,
    uf: Optional[str] = None,
    ano_agricola: Optional[str] = None,
    page: int = Query(1, ge=1, description="Número da página"),
    page_size: int = Query(20, ge=1, le=100, description="Itens por página (máximo: 100)"),
) -> PaginatedResponse:
    """
    Obtém as estimativas de safra da CONAB a partir de views Parquet paginadas.
    Permite filtros opcionais por cultura, unidade federativa (UF) e ano agrícola (ex: '2023/24').
    """
    sql = """
        SELECT 
            row_number() over () as id_conab,
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
    params = []
    if cultura:
        sql += " AND c.nome_padronizado = ?"
        params.append(cultura.lower().strip())
    if uf:
        sql += " AND p.uf = ?"
        params.append(uf.upper().strip())
    if ano_agricola:
        sql += " AND p.ano_agricola = ?"
        params.append(ano_agricola.strip())

    return paginate_query(sql, page, page_size, params)


@router.get("/sigef", response_model=PaginatedResponse[SigefProducaoSchema])
def get_sigef(
    cultura: Optional[str] = None,
    uf: Optional[str] = None,
    safra: Optional[str] = None,
    page: int = Query(1, ge=1, description="Número da página"),
    page_size: int = Query(20, ge=1, le=100, description="Itens por página (máximo: 100)"),
) -> PaginatedResponse:
    """
    Obtém os dados do SIGEF de forma paginada a partir de views Parquet.
    Permite filtros opcionais por cultura, unidade federativa (UF) e safra.
    """
    sql = """
        SELECT 
            row_number() over () as id_sigef_producao,
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
    params = []
    if cultura:
        sql += " AND c.nome_padronizado = ?"
        params.append(cultura.lower().strip())
    if uf:
        sql += " AND m.uf = ?"
        params.append(uf.upper().strip())
    if safra:
        sql += " AND s.safra = ?"
        params.append(safra.strip())

    return paginate_query(sql, page, page_size, params)
