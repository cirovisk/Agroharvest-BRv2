"""
Router de Analytics — Endpoints compostos que cruzam múltiplas tabelas do Star Schema.
Todos os endpoints são somente-leitura e agregam dados de 2+ tabelas fato usando DuckDB OLAP.
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
import math
import numpy as np

from db.duck_manager import duck_db
from api.schemas import (
    RaioXAgroMunicipalSchema, ProducaoPAMSimplesSchema,
    DossieInsumosCulturaSchema,
    ViabilidadeEconomicaSchema,
    JanelaDeAplicacaoSchema,
    AuditoriaEstimativasSchema,
)

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get(
    "/raio-x-municipal",
    response_model=RaioXAgroMunicipalSchema,
    summary="Raio-X Agroclimático de um município/cultura/ano",
)
def raio_x_municipal(codigo_ibge: str, cultura: str, ano: int):
    # Dimensões
    mun_df = duck_db.execute_query(f"SELECT id_municipio, nome, uf FROM dim_municipio WHERE codigo_ibge = '{codigo_ibge}'")
    if mun_df.empty: raise HTTPException(404, detail="Município não encontrado.")
    mun = mun_df.iloc[0]

    cult_df = duck_db.execute_query(f"SELECT id_cultura, nome_padronizado FROM dim_cultura WHERE nome_padronizado = '{cultura.lower()}'")
    if cult_df.empty: raise HTTPException(404, detail="Cultura não encontrada.")
    cult = cult_df.iloc[0]

    id_mun, id_cult = mun.id_municipio, cult.id_cultura

    # PAM
    pam_df = duck_db.execute_query(f"""
        SELECT area_plantada_ha, qtde_produzida_ton 
        FROM fato_producao_pam 
        WHERE id_municipio = {id_mun} AND id_cultura = {id_cult} AND ano = {ano}
    """)
    pam = pam_df.iloc[0] if not pam_df.empty else None

    # ZARC
    zarc_df = duck_db.execute_query(f"""
        SELECT risco_climatico, count(*) as c 
        FROM fato_risco_zarc 
        WHERE id_municipio = {id_mun} AND id_cultura = {id_cult} 
        GROUP BY risco_climatico ORDER BY c DESC LIMIT 1
    """)
    risco = str(zarc_df.iloc[0]['risco_climatico']) if not zarc_df.empty else None

    # Clima
    clima_df = duck_db.execute_query(f"""
        SELECT avg(temp_media_c) as t, avg(umidade_media) as u, sum(precipitacao_total_mm) as p
        FROM fato_meteorologia
        WHERE id_municipio = {id_mun} AND year(data) = {ano}
    """)
    clima = clima_df.iloc[0] if not clima_df.empty else None

    resultado_safra = ProducaoPAMSimplesSchema(
        ano=ano,
        area_plantada_ha=float(pam.area_plantada_ha) if pam is not None and not np.isnan(pam.area_plantada_ha) else None,
        qtde_produzida_ton=float(pam.qtde_produzida_ton) if pam is not None and not np.isnan(pam.qtde_produzida_ton) else None,
    )
    
    resumo_climatico = None
    if clima is not None and not np.isnan(clima.t):
        resumo_climatico = {
            "temp_media_c": round(clima.t, 2),
            "umidade_media": round(clima.u, 2),
            "precipitacao_anual_mm": round(clima.p, 2)
        }

    ocorreu_quebra = None
    if pam is not None and not np.isnan(pam.area_plantada_ha) and not np.isnan(pam.qtde_produzida_ton):
        produtividade = pam.qtde_produzida_ton / pam.area_plantada_ha if pam.area_plantada_ha > 0 else 0
        ocorreu_quebra = produtividade < 1.0

    return RaioXAgroMunicipalSchema(
        municipio=mun.nome,
        uf=mun.uf or "",
        cultura=cult.nome_padronizado,
        ano=ano,
        resultado_safra=resultado_safra,
        risco_zarc_predominante=risco,
        resumo_climatico=resumo_climatico,
        ocorreu_quebra_safra=ocorreu_quebra,
    )


@router.get(
    "/dossie-insumos/{cultura}",
    response_model=DossieInsumosCulturaSchema,
    summary="Dossiê completo de insumos: cultivares registradas, sementes e defensivos",
)
def dossie_insumos(cultura: str):
    cult_df = duck_db.execute_query(f"SELECT id_cultura, nome_padronizado FROM dim_cultura WHERE nome_padronizado = '{cultura.lower()}'")
    if cult_df.empty: raise HTTPException(404, detail="Cultura não encontrada.")
    id_cult = cult_df.iloc[0].id_cultura
    nome_cultura = cult_df.iloc[0].nome_padronizado

    # RNC
    rnc = duck_db.execute_query(f"""
        SELECT count(DISTINCT nr_registro) as count 
        FROM fato_registro_cultivares 
        WHERE id_cultura = {id_cult} AND situacao ILIKE '%REGISTRAD%'
    """)
    cultivares_ativos = int(rnc.iloc[0]['count']) if not rnc.empty else 0

    # SIGEF
    sigef = duck_db.execute_query(f"SELECT sum(producao_bruta_t) as vol FROM fato_sigef_producao WHERE id_cultura = {id_cult}")
    vol_sementes = float(sigef.iloc[0]['vol']) if not sigef.empty and not np.isnan(sigef.iloc[0]['vol']) else None

    # Defensivos (Agrofit)
    def_df = duck_db.execute_query(f"""
        SELECT DISTINCT marca_comercial 
        FROM fato_agrofit 
        WHERE id_cultura = {id_cult} LIMIT 20
    """)
    defensivos = def_df['marca_comercial'].tolist() if not def_df.empty else []

    # Pragas
    pragas_df = duck_db.execute_query(f"""
        SELECT praga_comum, count(*) as n 
        FROM fato_agrofit 
        WHERE id_cultura = {id_cult} AND praga_comum IS NOT NULL 
        GROUP BY praga_comum ORDER BY n DESC LIMIT 10
    """)
    pragas = pragas_df['praga_comum'].tolist() if not pragas_df.empty else []

    # Grau Tecnologia
    total_def_df = duck_db.execute_query(f"SELECT count(id_agrofit) as c FROM fato_agrofit WHERE id_cultura = {id_cult}")
    total_def = int(total_def_df.iloc[0]['c']) if not total_def_df.empty else 0
    
    if total_def >= 100: grau = "Alto"
    elif total_def >= 30: grau = "Médio"
    else: grau = "Baixo"

    return DossieInsumosCulturaSchema(
        cultura=nome_cultura,
        cultivares_ativos=cultivares_ativos,
        volume_sementes_sigef_ton=round(vol_sementes, 2) if vol_sementes else None,
        defensivos_recomendados=defensivos,
        principais_pragas_alvo=pragas,
        grau_de_tecnologia=grau,
    )


@router.get(
    "/viabilidade-economica",
    response_model=ViabilidadeEconomicaSchema,
    summary="Estimativa de receita bruta por cultura/UF/ano cruzando produção e preços",
)
def viabilidade_economica(cultura: str, uf: str, ano: int):
    cult_df = duck_db.execute_query(f"SELECT id_cultura, nome_padronizado FROM dim_cultura WHERE nome_padronizado = '{cultura.lower()}'")
    if cult_df.empty: raise HTTPException(404, detail="Cultura não encontrada.")
    id_cult = cult_df.iloc[0].id_cultura
    uf = uf.upper()

    # PAM Produção
    pam_df = duck_db.execute_query(f"""
        SELECT sum(p.qtde_produzida_ton) as prod, sum(p.area_plantada_ha) as area
        FROM fato_producao_pam p
        JOIN dim_municipio m ON p.id_municipio = m.id_municipio
        WHERE p.id_cultura = {id_cult} AND m.uf = '{uf}' AND p.ano = {ano}
    """)
    pam = pam_df.iloc[0] if not pam_df.empty else None
    producao_ton = float(pam.prod) if pam is not None and not np.isnan(pam.prod) else None
    area_ha = float(pam.area) if pam is not None and not np.isnan(pam.area) else None

    # Preço CONAB
    preco_df = duck_db.execute_query(f"""
        SELECT avg(valor_kg) as preco
        FROM fato_precos_conab_mensal
        WHERE id_cultura = {id_cult} AND uf = '{uf}' AND ano = {ano}
    """)
    preco_kg = float(preco_df.iloc[0]['preco']) if not preco_df.empty and not np.isnan(preco_df.iloc[0]['preco']) else None
    preco_ton = preco_kg * 1000 if preco_kg else None

    vgb = None
    if producao_ton and preco_ton:
        vgb = round((producao_ton * preco_ton) / 1_000_000, 2)

    renda_ha = None
    if vgb and area_ha and area_ha > 0:
        renda_ha = round((vgb * 1_000_000) / area_ha, 2)

    return ViabilidadeEconomicaSchema(
        ano=ano,
        cultura=cult_df.iloc[0].nome_padronizado,
        uf=uf,
        producao_total_ton=producao_ton,
        preco_medio_anual_ton=round(preco_ton, 2) if preco_ton else None,
        valor_teto_atingido=round(preco_ton * 1.2, 2) if preco_ton else None,
        vgb_apurado_milhoes=vgb,
        renda_media_hectare=renda_ha,
    )


@router.get(
    "/janela-aplicacao",
    response_model=JanelaDeAplicacaoSchema,
    summary="Avalia disponibilidade de insumos e condições climáticas para aplicação",
)
def janela_aplicacao(codigo_ibge: str, ano: int, mes: int):
    mun_df = duck_db.execute_query(f"SELECT id_municipio, nome, uf FROM dim_municipio WHERE codigo_ibge = '{codigo_ibge}'")
    if mun_df.empty: raise HTTPException(404, detail="Município não encontrado.")
    mun = mun_df.iloc[0]

    # Fertilizantes SIPEAGRO
    estab_df = duck_db.execute_query(f"""
        SELECT count(id_fertilizante) as c 
        FROM fato_fertilizantes_estabelecimentos 
        WHERE uf = '{mun.uf}' AND status_registro ILIKE '%ATIVO%'
    """)
    estab_count = int(estab_df.iloc[0]['c']) if not estab_df.empty else 0

    # Meteorologia
    chuva_df = duck_db.execute_query(f"""
        SELECT sum(precipitacao_total_mm) as p
        FROM fato_meteorologia
        WHERE id_municipio = {mun.id_municipio} AND year(data) = {ano} AND month(data) = {mes}
    """)
    chuva_mm = float(chuva_df.iloc[0]['p']) if not chuva_df.empty and not np.isnan(chuva_df.iloc[0]['p']) else None

    janela_perfeita = None
    if chuva_mm is not None:
        janela_perfeita = (50.0 <= chuva_mm <= 200.0) and estab_count >= 3

    if estab_count >= 50: capacidade = "Alta"
    elif estab_count >= 15: capacidade = "Média"
    elif estab_count >= 1: capacidade = "Baixa"
    else: capacidade = "Sem cobertura"

    return JanelaDeAplicacaoSchema(
        municipio=mun.nome,
        uf=mun.uf or "",
        mes_referencia=f"{ano}-{mes:02d}",
        estabelecimentos_insumos=estab_count,
        acumulado_chuvas_mm=round(chuva_mm, 2) if chuva_mm is not None else None,
        janela_perfeita_aplicacao=janela_perfeita,
        capacidade_de_atendimento=capacidade,
    )


@router.get(
    "/auditoria-estimativas",
    response_model=List[AuditoriaEstimativasSchema],
    summary="Compara estimativas CONAB com resultados reais do IBGE/PAM por UF e safra",
)
def auditoria_estimativas(cultura: str, uf: Optional[str] = None):
    cult_df = duck_db.execute_query(f"SELECT id_cultura, nome_padronizado FROM dim_cultura WHERE nome_padronizado = '{cultura.lower()}'")
    if cult_df.empty: raise HTTPException(404, detail="Cultura não encontrada.")
    id_cult = cult_df.iloc[0].id_cultura
    nome_cult = cult_df.iloc[0].nome_padronizado

    uf_filter = f"AND uf = '{uf.upper()}'" if uf else ""
    
    # CONAB Estimativas
    conab_df = duck_db.execute_query(f"""
        SELECT uf, ano_agricola, sum(producao_mil_t) as estimativa
        FROM fato_producao_conab
        WHERE id_cultura = {id_cult} {uf_filter}
        GROUP BY uf, ano_agricola
        LIMIT 50
    """)

    results = []
    for _, row in conab_df.iterrows():
        ano_agricola = row['ano_agricola']
        try:
            ano_pam = int(str(ano_agricola).split("/")[0])
        except: continue

        # PAM Realizado
        pam_df = duck_db.execute_query(f"""
            SELECT sum(p.qtde_produzida_ton) as prod
            FROM fato_producao_pam p
            JOIN dim_municipio m ON p.id_municipio = m.id_municipio
            WHERE p.id_cultura = {id_cult} AND m.uf = '{row.uf}' AND p.ano = {ano_pam}
        """)
        
        realizado_ton = float(pam_df.iloc[0]['prod']) if not pam_df.empty and not np.isnan(pam_df.iloc[0]['prod']) else None
        realizado_mil_t = (realizado_ton / 1000.0) if realizado_ton else None
        estimativa_mil_t = float(row['estimativa']) if not np.isnan(row['estimativa']) else None

        variacao = None
        acuracidade = None
        if realizado_mil_t is not None and estimativa_mil_t and estimativa_mil_t > 0:
            variacao = round(((realizado_mil_t - estimativa_mil_t) / estimativa_mil_t) * 100, 2)
            abs_var = abs(variacao)
            if abs_var <= 5: acuracidade = "Excelente (≤5%)"
            elif abs_var <= 15: acuracidade = "Boa (5–15%)"
            elif abs_var <= 30: acuracidade = "Regular (15–30%)"
            else: acuracidade = "Fraca (>30%)"

        results.append(
            AuditoriaEstimativasSchema(
                ano_safra=ano_agricola,
                uf=row.uf,
                cultura=nome_cult,
                estimativa_conab_mil_t=round(estimativa_mil_t, 2) if estimativa_mil_t else None,
                realizado_ibge_pam_mil_t=round(realizado_mil_t, 2) if realizado_mil_t else None,
                variacao_margem_erro=variacao,
                acuracidade_relatorio=acuracidade,
            )
        )

    return results
