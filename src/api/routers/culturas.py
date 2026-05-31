from typing import List

from fastapi import APIRouter, HTTPException, Query

from api.schemas import CulturaBaseSchema, PaginatedResponse
from api.utils import paginate_query
from db.duck_manager import duck_db

router = APIRouter(prefix="/culturas", tags=["Culturas"])


@router.get("/", response_model=PaginatedResponse[CulturaBaseSchema])
def list_culturas(
    page: int = Query(1, ge=1, description="Número da página"),
    page_size: int = Query(20, ge=1, le=100, description="Itens por página (máximo: 100)"),
):
    sql = "SELECT id_cultura, nome_padronizado FROM dim_cultura"
    return paginate_query(sql, page, page_size)


@router.get("/{cultura}", response_model=CulturaBaseSchema)
def get_cultura(cultura: str):
    sql = "SELECT id_cultura, nome_padronizado FROM dim_cultura WHERE nome_padronizado = ?"
    df = duck_db.execute_query(sql, (cultura.lower().strip(),))
    if df.empty:
        raise HTTPException(status_code=404, detail="Cultura não encontrada")
    return df.to_dict(orient="records")[0]
