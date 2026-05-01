import pytest
import pandas as pd
from src.db.duck_manager import DuckManager
from src.pipeline.dimensions import preencher_dimensao_cultura, preencher_dimensao_municipio

def test_duck_manager_refresh_views(tmp_path):
    # Cria um parquet fake
    storage = tmp_path / "storage"
    table_dir = storage / "fato_teste"
    table_dir.mkdir(parents=True)
    
    df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    df.to_parquet(table_dir / "data.parquet")
    
    # Init manager
    manager = DuckManager(storage_path=str(storage), db_file=":memory:")
    
    # Verifica se a view foi criada
    res = manager.execute_query("SELECT count(*) as c FROM fato_teste")
    assert int(res.iloc[0]['c']) == 2

def test_dimensao_cultura_duckdb(duck_conn):
    culturas = ["Soja", "Milho", "Trigo"]
    mapping = preencher_dimensao_cultura(duck_conn, culturas)
    
    assert "soja" in mapping
    assert "milho" in mapping
    assert isinstance(mapping["soja"], int)
    
    # Teste de idempotência
    mapping2 = preencher_dimensao_cultura(duck_conn, ["SOJA", "Arroz"])
    assert mapping["soja"] == mapping2["soja"]
    assert "arroz" in mapping2

def test_dimensao_municipio_duckdb(duck_conn):
    df_pam = pd.DataFrame({
        "cod_municipio_ibge": ["1200013"],
        "municipio_nome": ["Municipio A"],
        "uf": ["AC"]
    })
    
    map_ibge, map_nome = preencher_dimensao_municipio(duck_conn, df_pam=df_pam)
    
    assert "1200013" in map_ibge
    assert ("municipio a", "AC") in map_nome
    assert map_ibge["1200013"] == map_nome[("municipio a", "AC")]
