"""
Utilitários compartilhados: normalização de strings,
lookup de cultura, e mapeamento de município por nome.
"""

import logging
import unicodedata
import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Normalização de Strings
# ---------------------------------------------------------------------------

def normalize_string(series: pd.Series) -> pd.Series:
    """Normalização: Padronização de nomes (remuneração de acentos, lowercase)."""
    def remove_accents(input_str):
        if not isinstance(input_str, str):
            return input_str
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        return u"".join([c for c in nfkd_form if not unicodedata.combining(c)]).lower()

    return series.apply(remove_accents).str.strip()


# ---------------------------------------------------------------------------
# Lookup de Cultura (com Sinônimos)
# ---------------------------------------------------------------------------

def get_cultura_id(nome_cultura, mapping):
    if not nome_cultura: return None

    def norm(s):
        s = str(s).lower().strip()
        s = "".join(c for c in unicodedata.normalize('NFKD', s) if unicodedata.category(c) != 'Mn')
        return s.replace("-", " ").replace("_", " ")

    # Dicionário de Sinônimos Científicos (SIGEF/MAPA -> Popular)
    SYNONYMS = {
        "glycine max": "soja",
        "zea mays": "milho",
        "triticum aestivum": "trigo",
        "gossypium hirsutum": "algodao",
        "avena strigosa": "aveia",
        "avena sativa": "aveia",
        "saccharum": "cana-de-acucar"
    }

    # Tenta match exato primeiro (antes de normalizar)
    if nome_cultura in mapping: return mapping[nome_cultura]

    nombre_norm = norm(nome_cultura)
    
    # Aplica Tradução de Sinônimos
    for syn, target in SYNONYMS.items():
        if syn in nombre_norm:
            nombre_norm = target
            break

    for alvo, cid in mapping.items():
        alvo_norm = norm(alvo)
        # Match de palavra inteira ou exato para evitar erros como strigosa -> trigo
        if f" {alvo_norm} " in f" {nombre_norm} " or f" {nombre_norm} " in f" {alvo_norm} ":
            return cid
        if alvo_norm == nombre_norm:
            return cid
    return None


# ---------------------------------------------------------------------------
# Mapeamento de Município por Nome
# ---------------------------------------------------------------------------

def map_municipio_by_name(df, map_mun_name):
    """Lookup vectorizado de id_municipio via (nome, uf) — substitui apply(axis=1)."""
    has_mun = df["municipio"].notna() & df["uf"].notna()
    keys = df["municipio"].str.lower().str.strip() + "|" + df["uf"].str.upper()
    lookup = {f"{n}|{u}": mid for (n, u), mid in map_mun_name.items()}
    return keys.map(lookup).where(has_mun)
