"""
Funções de gerenciamento de dimensões para DuckDB.
Responsável por popular dim_cultura, dim_municipio e dim_mantenedor.
"""

import logging
import requests
import pandas as pd

log = logging.getLogger(__name__)


def init_dimensions(conn):
    """Cria tabelas de dimensão no DuckDB se não existirem."""
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_cultura;
        CREATE TABLE IF NOT EXISTS dim_cultura (
            id_cultura INTEGER DEFAULT nextval('seq_cultura') PRIMARY KEY,
            nome_padronizado VARCHAR UNIQUE
        );

        CREATE SEQUENCE IF NOT EXISTS seq_municipio;
        CREATE TABLE IF NOT EXISTS dim_municipio (
            id_municipio INTEGER DEFAULT nextval('seq_municipio') PRIMARY KEY,
            codigo_ibge VARCHAR UNIQUE,
            nome VARCHAR,
            uf VARCHAR
        );

        CREATE SEQUENCE IF NOT EXISTS seq_mantenedor;
        CREATE TABLE IF NOT EXISTS dim_mantenedor (
            id_mantenedor INTEGER DEFAULT nextval('seq_mantenedor') PRIMARY KEY,
            nome VARCHAR UNIQUE,
            setor VARCHAR
        );
    """)


def preencher_dimensao_cultura(conn, culturas_lista):
    init_dimensions(conn)
    df_insert = pd.DataFrame({"nome_padronizado": [c.strip().lower() for c in culturas_lista]})
    
    try:
        conn.execute("INSERT OR IGNORE INTO dim_cultura (nome_padronizado) SELECT nome_padronizado FROM df_insert")
    except Exception as e:
        log.error(f"Erro ao inserir culturas: {e}")
        
    df = conn.execute("SELECT nome_padronizado, id_cultura FROM dim_cultura").df()
    return df.set_index("nome_padronizado")["id_cultura"].to_dict()


def preencher_dimensao_mantenedor(conn, df_cult):
    init_dimensions(conn)
    mant_map = {}
    if df_cult.empty or "mantenedor" not in df_cult.columns:
        return mant_map

    col_setor = "SETOR" if "SETOR" in df_cult.columns else "setor" if "setor" in df_cult.columns else None
    cols = ["mantenedor"] + ([col_setor] if col_setor else [])
    unique_mants = df_cult[cols].drop_duplicates().dropna(subset=["mantenedor"])
    
    if unique_mants.empty:
        return mant_map
        
    try:
        if col_setor:
            unique_mants = unique_mants.rename(columns={"mantenedor": "nome", col_setor: "setor"})
            conn.execute("INSERT OR IGNORE INTO dim_mantenedor (nome, setor) SELECT nome, setor FROM unique_mants")
        else:
            unique_mants = unique_mants.rename(columns={"mantenedor": "nome"})
            conn.execute("INSERT OR IGNORE INTO dim_mantenedor (nome) SELECT nome FROM unique_mants")
    except Exception as e:
        log.error(f"Erro ao inserir mantenedores: {e}")
        
    df = conn.execute("SELECT nome, id_mantenedor FROM dim_mantenedor").df()
    return df.set_index("nome")["id_mantenedor"].to_dict()


def carregar_municipios_completo_ibge(conn):
    init_dimensions(conn)
    log.info("Buscando lista completa de municípios na API do IBGE...")
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        muns_data = resp.json()
    except Exception as e:
        log.error(f"Falha ao buscar municípios no IBGE: {e}")
        return preencher_dimensao_municipio(conn)

    records = []
    for m in muns_data:
        cod = str(m["id"])
        nome = m["nome"]
        
        uf = None
        try:
            uf = m.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("sigla")
            if not uf:
                uf = m.get("regiao-imediata", {}).get("regiao-intermediaria", {}).get("UF", {}).get("sigla")
        except:
            uf = "XX"
            
        uf = str(uf).upper() if uf else "XX"
        records.append({"codigo_ibge": cod, "nome": nome, "uf": uf})
    
    df_muns = pd.DataFrame(records)
    if not df_muns.empty:
        try:
            conn.execute("INSERT OR IGNORE INTO dim_municipio (codigo_ibge, nome, uf) SELECT codigo_ibge, nome, uf FROM df_muns")
            log.info(f"DimMunicipio: Municipios validados com sucesso no DuckDB.")
        except Exception as e:
            log.error(f"Erro ao inserir municipios do IBGE: {e}")
            
    return preencher_dimensao_municipio(conn)


def preencher_dimensao_municipio(conn, df_pam=pd.DataFrame(), df_zarc=pd.DataFrame()):
    init_dimensions(conn)
    
    # Para simplicidade e performance, vamos focar em carregar do IBGE.
    # O DuckDB vai ignorar duplicados pelo `UNIQUE` do codigo_ibge.
    if not df_pam.empty and "cod_municipio_ibge" in df_pam.columns:
        pam_muns = df_pam[["cod_municipio_ibge", "municipio_nome", "uf"]].drop_duplicates().dropna(subset=["cod_municipio_ibge"])
        pam_muns["cod_municipio_ibge"] = pam_muns["cod_municipio_ibge"].astype(str).str[:7]
        pam_muns = pam_muns.rename(columns={"cod_municipio_ibge": "codigo_ibge", "municipio_nome": "nome"})
        try:
            conn.execute("INSERT OR IGNORE INTO dim_municipio (codigo_ibge, nome, uf) SELECT codigo_ibge, nome, uf FROM pam_muns")
        except: pass
            
    if not df_zarc.empty and "cod_municipio_ibge" in df_zarc.columns:
        zarc_muns = df_zarc[["cod_municipio_ibge", "municipio", "uf"]].drop_duplicates().dropna(subset=["cod_municipio_ibge"])
        zarc_muns["cod_municipio_ibge"] = zarc_muns["cod_municipio_ibge"].astype(str).str[:7]
        zarc_muns = zarc_muns.rename(columns={"cod_municipio_ibge": "codigo_ibge", "municipio": "nome"})
        try:
            conn.execute("INSERT OR IGNORE INTO dim_municipio (codigo_ibge, nome, uf) SELECT codigo_ibge, nome, uf FROM zarc_muns")
        except: pass

    # Fetch results
    df = conn.execute("SELECT id_municipio, codigo_ibge, nome, uf FROM dim_municipio").df()
    
    mun_map_ibge = df.set_index("codigo_ibge")["id_municipio"].to_dict()
    
    # Criar o map_nome: (nome.lower(), uf) -> id
    mun_map_name = {}
    for _, row in df.iterrows():
        if pd.notna(row["uf"]) and pd.notna(row["nome"]):
            mun_map_name[(str(row["nome"]).lower().strip(), str(row["uf"]).strip().upper())] = row["id_municipio"]
            
    return mun_map_ibge, mun_map_name
