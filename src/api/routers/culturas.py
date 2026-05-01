from fastapi import APIRouter, HTTPException
from typing import List

from db.duck_manager import duck_db
from api.schemas import CulturaBaseSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/culturas", tags=["Culturas"])

@router.get("/", response_model=PaginatedResponse[CulturaBaseSchema])
def list_culturas(page: int = 1, page_size: int = 20):
    sql = "SELECT id_cultura, nome_padronizado FROM dim_cultura"
    return paginate_query(sql, page, page_size)

@router.get("/{cultura}", response_model=CulturaBaseSchema)
def get_cultura(cultura: str):
    sql = f"SELECT id_cultura, nome_padronizado FROM dim_cultura WHERE nome_padronizado = '{cultura.lower().strip()}'"
    df = duck_db.execute_query(sql)
    if df.empty:
        raise HTTPException(status_code=404, detail="Cultura não encontrada")
    return df.to_dict(orient="records")[0]
