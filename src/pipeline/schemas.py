"""
Schemas de validação Pandera para dados limpos do pipeline.

Cada schema valida a saída do clean() de uma fonte antes do load().
Se o governo mudar nomes de colunas ou tipos, o pipeline levanta um
SchemaError explícito dizendo exatamente o que mudou — em vez de
inserir dados malformados ou crashar com erro genérico do Pandas.

Uso:
    schema.validate(df, lazy=True)  # lazy=True coleta TODOS os erros
"""

import pandera as pa
from pandera import Check, Column, DataFrameSchema

# CONAB

ConabProducaoSchema = DataFrameSchema(
    {
        "cultura": Column(str, nullable=False),
        "uf": Column(str, nullable=True),
        "area_plantada_mil_ha": Column(float, Check.ge(0), nullable=True),
        "producao_mil_t": Column(float, Check.ge(0), nullable=True),
        "produtividade_t_ha": Column(float, Check.ge(0), nullable=True),
    },
    coerce=True,
    strict=False,  # Permite colunas extras (ex: ano_agricola, safra)
    name="ConabProducaoSchema",
)

ConabPrecosSchema = DataFrameSchema(
    {
        "cultura": Column(str, nullable=False),
        "uf": Column(str, nullable=True),
        "ano": Column(int, Check.gt(1900), nullable=True),
        "valor_kg": Column(float, Check.ge(0), nullable=True),
    },
    coerce=True,
    strict=False,
    name="ConabPrecosSchema",
)


# SIDRA / PAM (IBGE)

SidraSchema = DataFrameSchema(
    {
        "cod_municipio_ibge": Column(str, nullable=False),
        "cultura": Column(str, nullable=False),
        "ano": Column(str, nullable=True),
        "area_plantada_ha": Column(float, nullable=True),
        "area_colhida_ha": Column(float, nullable=True),
        "qtde_produzida_ton": Column(float, nullable=True),
        "valor_producao_mil_reais": Column(float, nullable=True),
    },
    coerce=True,
    strict=False,
    name="SidraSchema",
)


# ZARC (MAPA)

ZarcSchema = DataFrameSchema(
    {
        "cod_municipio_ibge": Column(nullable=False),
        "cultura": Column(str, nullable=False),
    },
    coerce=True,
    strict=False,  # ZARC tem muitas colunas dec1..dec36 dinâmicas
    name="ZarcSchema",
)


# Agrofit (MAPA)

AgrofitSchema = DataFrameSchema(
    {
        "nr_registro": Column(str, nullable=True),
        "marca_comercial": Column(str, nullable=True),
        "cultura": Column(str, nullable=False),
    },
    coerce=True,
    strict=False,
    name="AgrofitSchema",
)


# Cultivares / SNPC (MAPA)

CultivaresSchema = DataFrameSchema(
    {
        "cultivar": Column(str, nullable=False),
        "cultura": Column(str, nullable=False),
    },
    coerce=True,
    strict=False,
    name="CultivaresSchema",
)


# Fertilizantes / SIPEAGRO (MAPA)

FertilizantesSchema = DataFrameSchema(
    {
        "uf": Column(str, nullable=True),
        "municipio": Column(str, nullable=True),
        "nr_registro_estabelecimento": Column(str, nullable=True),
    },
    coerce=True,
    strict=False,
    name="FertilizantesSchema",
)


# SIGEF (MAPA)

SigefProducaoSchema = DataFrameSchema(
    {
        "cultura": Column(str, nullable=False),
        "area_ha": Column(float, Check.ge(0), nullable=True),
    },
    coerce=True,
    strict=False,
    name="SigefProducaoSchema",
)

SigefReservaSchema = DataFrameSchema(
    {
        "cultura": Column(str, nullable=False),
        "area_total_ha": Column(float, Check.ge(0), nullable=True),
    },
    coerce=True,
    strict=False,
    name="SigefReservaSchema",
)


# Open-Meteo

OpenMeteoSchema = DataFrameSchema(
    {
        "data": Column("datetime64[ns]", nullable=False),
        "id_municipio": Column(nullable=False),
        "temp_max_c": Column(float, nullable=True),
        "temp_min_c": Column(float, nullable=True),
        "precipitacao_total_mm": Column(float, nullable=True),
    },
    coerce=True,
    strict=False,
    name="OpenMeteoSchema",
)


# NDVI Satélite

NdviSchema = DataFrameSchema(
    {
        "codigo_ibge": Column(str, nullable=False),
        "ano": Column(int, Check.gt(1900), nullable=False),
        "ndvi_max_safra": Column(float, Check.between(-1.0, 1.0), nullable=True),
        "ndvi_mean_safra": Column(float, Check.between(-1.0, 1.0), nullable=True),
    },
    coerce=True,
    strict=False,
    name="NdviSchema",
)


# Registry: mapeia nomes de schema para schemas (para lookup dinâmico)

SCHEMA_REGISTRY = {
    "conab_producao": ConabProducaoSchema,
    "conab_precos": ConabPrecosSchema,
    "sidra": SidraSchema,
    "zarc": ZarcSchema,
    "agrofit": AgrofitSchema,
    "cultivares": CultivaresSchema,
    "fertilizantes": FertilizantesSchema,
    "sigef_producao": SigefProducaoSchema,
    "sigef_reserva": SigefReservaSchema,
    "open_meteo": OpenMeteoSchema,
    "ndvi": NdviSchema,
}
