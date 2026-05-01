from fastapi import APIRouter, HTTPException
from typing import Optional

from db.duck_manager import duck_db
from api.schemas import MunicipioBaseSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/municipios", tags=["Municípios"])

@router.get("/", response_model=PaginatedResponse[MunicipioBaseSchema])
def list_municipios(uf: Optional[str] = None, page: int = 1, page_size: int = 20):
    sql = "SELECT id_municipio, codigo_ibge, nome, uf FROM dim_municipio WHERE 1=1"
    if uf:
        sql += f" AND uf = '{uf.upper().strip()}'"
        
    return paginate_query(sql, page, page_size)

@router.get("/{codigo_ibge}", response_model=MunicipioBaseSchema)
def get_municipio(codigo_ibge: str):
    sql = f"SELECT id_municipio, codigo_ibge, nome, uf FROM dim_municipio WHERE codigo_ibge = '{codigo_ibge.strip()}'"
    df = duck_db.execute_query(sql)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Município não encontrado")
    return df.to_dict(orient="records")[0]
