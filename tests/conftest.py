import pytest
import pandas as pd
import sys
import os
import duckdb
from pathlib import Path

# Adiciona src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.duck_manager import duck_db

@pytest.fixture(scope="function")
def duck_conn():
    """Conexão DuckDB em memória para testes."""
    conn = duckdb.connect(database=":memory:")
    # Inicializa dimensões
    from pipeline.dimensions import init_dimensions
    init_dimensions(conn)
    
    # Inicializa tabelas de fatos mockadas para os testes rodarem
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fato_producao_pam (
            ano INTEGER,
            area_plantada_ha REAL,
            area_colhida_ha REAL,
            qtde_produzida_ton REAL,
            valor_producao_mil_reais REAL,
            id_cultura INTEGER,
            id_municipio INTEGER
        );
        CREATE TABLE IF NOT EXISTS fato_risco_zarc (
            periodo_plantio VARCHAR,
            tipo_solo VARCHAR,
            risco_climatico VARCHAR,
            id_cultura INTEGER,
            id_municipio INTEGER
        );
        CREATE TABLE IF NOT EXISTS fato_meteorologia (
            data DATE,
            precipitacao_total_mm REAL,
            temp_media_c REAL,
            umidade_media REAL,
            id_municipio INTEGER
        );
        CREATE TABLE IF NOT EXISTS fato_registro_cultivares (
            nr_registro VARCHAR,
            id_cultura INTEGER,
            situacao VARCHAR
        );
        CREATE TABLE IF NOT EXISTS fato_sigef_producao (
            producao_bruta_t REAL,
            id_cultura INTEGER,
            safra VARCHAR,
            especie VARCHAR,
            categoria VARCHAR,
            area_ha REAL,
            producao_est_t REAL,
            id_municipio INTEGER
        );
        CREATE TABLE IF NOT EXISTS fato_agrofit (
            marca_comercial VARCHAR,
            id_cultura INTEGER,
            nr_registro VARCHAR,
            classe VARCHAR,
            praga_comum VARCHAR
        );
        CREATE TABLE IF NOT EXISTS fato_precos_conab_mensal (
            valor_kg REAL,
            id_cultura INTEGER,
            uf VARCHAR,
            ano INTEGER
        );
        CREATE TABLE IF NOT EXISTS fato_fertilizantes_estabelecimentos (
            uf VARCHAR,
            municipio VARCHAR,
            nr_registro_estabelecimento VARCHAR,
            razao_social VARCHAR,
            area_atuacao VARCHAR,
            atividade VARCHAR
        );
        CREATE TABLE IF NOT EXISTS fato_producao_conab (
            uf VARCHAR,
            ano_agricola VARCHAR,
            safra VARCHAR,
            area_plantada_mil_ha REAL,
            producao_mil_t REAL,
            produtividade_t_ha REAL,
            id_cultura INTEGER
        );
    """)
    return conn

@pytest.fixture(autouse=True)
def mock_duck_db(duck_conn):
    """Sobrescreve a conexão do singleton duck_db para usar o banco de testes."""
    original_conn = duck_db.conn
    duck_db.conn = duck_conn
    # Refresh views no banco de memória (caso existam arquivos parquet em data/storage)
    # Na verdade em testes o storage costuma estar vazio ou mockado.
    duck_db.refresh_views()
    yield
    duck_db.conn = original_conn

@pytest.fixture(scope="function")
def db_session(duck_conn):
    """
    Mock de sessão para compatibilidade com testes antigos.
    No DuckDB usamos a própria conexão.
    """
    return duck_conn

@pytest.fixture
def mock_sidra_raw():
    """Mock do JSON cru retornado pela API SIDRA."""
    data = {
        "D2N": ["Área plantada", "Área colhida", "Quantidade produzida", "Área plantada"],
        "V": ["1000", "-", "...", "2000"],
        "D1C": ["1200013", "1200013", "1200013", "1200054"],
        "D1N": ["Municipio A", "Municipio A", "Municipio A", "Municipio B"],
        "D3N": ["2022", "2022", "2022", "2022"],
        "cultura_raw": ["soja", "soja", "soja", "trigo"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_zarc_raw():
    """Mock de CSV consolidado do MAPA/Zarc cru."""
    data = {
        "cd_mun": [1200013, 1200054],
        "municipio": ["Municipio A", "Municipio B"],
        "cultura_raw": ["Soja", "Algodao"],
        "SOLO": ["Tipo 1", "Tipo 2"],
        "dec1": ["20%", "30%"],
        "dec2": ["40%", "10%"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_cultivares_raw():
    """Mock de CSV de cultivares do SNPC."""
    data = {
        "CULTIVAR": ["'BONANZA' / BONA", '"OURO"'],
        "NOME COMUM": ["Soja", "Milho"],
        "GRUPO DA ESPÉCIE": ["SOJA", "MILHO"],
        "SITUAÇÃO": ["REGISTRADA", "REGISTRADA"],
        "MANTENEDOR (REQUERENTE) (NOME)": ["EMBRAPA CERRADOS", "EMPRESA PRIVADA X"],
        "Nº FORMULÁRIO": ["123", "456"],
        "Nº REGISTRO": ["10", "11"],
        "DATA DO REGISTRO": ["02/01/2020", "15/10/2015"],
        "DATA DE VALIDADE DO REGISTRO": ["01/01/2035", "14/10/2030"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_conab_raw():
    """Mock dos dados crus da CONAB."""
    df_prod = pd.DataFrame({
        "ano_agricola": ["2023/24"],
        "dsc_safra_previsao": ["1ª Safra"],
        "uf": ["MT"],
        "produto": ["Milho"],
        "area_plantada_mil_ha": ["1000"],
        "producao_mil_t": ["5000"],
        "produtividade_mil_ha_mil_t": ["5.0"]
    })
    
    df_preco = pd.DataFrame({
        "produto": ["Milho"],
        "uf": ["MT"],
        "nom_municipio": ["Sorriso"],
        "cod_ibge": ["5107909"],
        "ano": ["2024"],
        "mes": ["1"],
        "valor_produto_kg": ["1,50"],
        "dsc_nivel_comercializacao": ["Produtor"]
    })
    
    return {
        "producao_estimativa": df_prod,
        "precos_mun_mensal": df_preco
    }

@pytest.fixture
def mock_agrofit_raw():
    """Mock de CSV do Agrofit cru."""
    data = {
        "NR_REGISTRO": ["12321", "45654"],
        "MARCA_COMERCIAL": ["MATA TUDO", "CRESCE MAIS"],
        "INGREDIENTE_ATIVO": ["Glifosato", "Nitrato"],
        "TITULAR_DE_REGISTRO": ["Empresa A", "Empresa B"],
        "CLASSE": ["Herbicida", "Fertilizante"],
        "SITUACAO": ["Registrado", "Registrado"],
        "CULTURA": ["Soja", "Milho"],
        "PRAGA_NOME_COMUM": ["Galinha", "Lagarta"]
    }
    return pd.DataFrame(data)
