"""
Testes para os schemas Pandera de validação de dados.

Valida que DataFrames corretos passam e DataFrames com erros
(colunas ausentes, tipos errados, valores negativos) falham
com mensagens explícitas.
"""

import pytest
import pandas as pd
import numpy as np
import pandera as pa

from pipeline.schemas import (
    ConabProducaoSchema,
    ConabPrecosSchema,
    SidraSchema,
    ZarcSchema,
    AgrofitSchema,
    CultivaresSchema,
    FertilizantesSchema,
    SigefProducaoSchema,
    SigefReservaSchema,
    OpenMeteoSchema,
)


# CONAB Produção

class TestConabProducaoSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "cultura": ["soja", "milho"],
            "uf": ["MT", "PR"],
            "area_plantada_mil_ha": [10.5, 20.3],
            "producao_mil_t": [30.0, 50.0],
            "produtividade_t_ha": [3.0, 2.5],
            "ano_agricola": ["2023/24", "2023/24"],  # extra col — strict=False
        })
        result = ConabProducaoSchema.validate(df)
        assert not result.empty

    def test_missing_cultura_fails(self):
        df = pd.DataFrame({
            "uf": ["MT"],
            "producao_mil_t": [30.0],
        })
        with pytest.raises(pa.errors.SchemaError):
            ConabProducaoSchema.validate(df)

    def test_negative_producao_fails(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
            "uf": ["MT"],
            "producao_mil_t": [-5.0],
        })
        with pytest.raises(pa.errors.SchemaError):
            ConabProducaoSchema.validate(df)


# CONAB Preços

class TestConabPrecosSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
            "uf": ["MT"],
            "ano": [2024],
            "valor_kg": [1.50],
        })
        result = ConabPrecosSchema.validate(df)
        assert not result.empty

    def test_negative_valor_fails(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
            "uf": ["MT"],
            "ano": [2024],
            "valor_kg": [-0.5],
        })
        with pytest.raises(pa.errors.SchemaError):
            ConabPrecosSchema.validate(df)


# SIDRA

class TestSidraSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "cod_municipio_ibge": ["1200013"],
            "cultura": ["soja"],
            "ano": ["2022"],
            "area_plantada_ha": [1000.0],
            "area_colhida_ha": [950.0],
            "qtde_produzida_ton": [3000.0],
            "valor_producao_mil_reais": [5000.0],
        })
        result = SidraSchema.validate(df)
        assert not result.empty

    def test_missing_cod_municipio_fails(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
            "ano": ["2022"],
        })
        with pytest.raises(pa.errors.SchemaError):
            SidraSchema.validate(df)


# ZARC

class TestZarcSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "cod_municipio_ibge": [1200013, 1200054],
            "cultura": ["soja", "milho"],
            "solo": ["Tipo 1", "Tipo 2"],
            "dec1": ["20%", "30%"],
        })
        result = ZarcSchema.validate(df)
        assert not result.empty

    def test_missing_cultura_fails(self):
        df = pd.DataFrame({
            "cod_municipio_ibge": [1200013],
        })
        with pytest.raises(pa.errors.SchemaError):
            ZarcSchema.validate(df)


# Agrofit

class TestAgrofitSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "nr_registro": ["12321"],
            "marca_comercial": ["MATA TUDO"],
            "cultura": ["soja"],
        })
        result = AgrofitSchema.validate(df)
        assert not result.empty

    def test_missing_cultura_fails(self):
        df = pd.DataFrame({
            "nr_registro": ["12321"],
            "marca_comercial": ["MATA TUDO"],
        })
        with pytest.raises(pa.errors.SchemaError):
            AgrofitSchema.validate(df)


# Cultivares

class TestCultivaresSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "cultivar": ["BONANZA"],
            "cultura": ["soja"],
            "nr_registro": ["10"],
            "situacao": ["REGISTRADA"],
        })
        result = CultivaresSchema.validate(df)
        assert not result.empty

    def test_missing_cultivar_fails(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
        })
        with pytest.raises(pa.errors.SchemaError):
            CultivaresSchema.validate(df)


# Fertilizantes

class TestFertilizantesSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "uf": ["SP"],
            "municipio": ["Campinas"],
            "nr_registro_estabelecimento": ["12345"],
        })
        result = FertilizantesSchema.validate(df)
        assert not result.empty


# SIGEF

class TestSigefProducaoSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
            "area_ha": [100.0],
            "safra": ["2023/2024"],
        })
        result = SigefProducaoSchema.validate(df)
        assert not result.empty

    def test_negative_area_fails(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
            "area_ha": [-50.0],
        })
        with pytest.raises(pa.errors.SchemaError):
            SigefProducaoSchema.validate(df)


class TestSigefReservaSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "cultura": ["soja"],
            "area_total_ha": [200.0],
        })
        result = SigefReservaSchema.validate(df)
        assert not result.empty


# Open-Meteo

class TestOpenMeteoSchema:
    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "data": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "id_municipio": [1, 2],
            "temp_max_c": [35.0, 33.0],
            "temp_min_c": [22.0, 20.0],
            "precipitacao_total_mm": [0.0, 10.5],
        })
        result = OpenMeteoSchema.validate(df)
        assert not result.empty

    def test_missing_data_column_fails(self):
        df = pd.DataFrame({
            "id_municipio": [1],
            "temp_max_c": [35.0],
        })
        with pytest.raises(pa.errors.SchemaError):
            OpenMeteoSchema.validate(df)


# Validação com BaseSource.validate()

class TestBaseSourceValidate:
    def test_validate_passthrough_no_schema(self):
        """Se schema=None, validate() retorna o DataFrame sem alterar."""
        from pipeline.base import BaseSource

        # Classe concreta mínima para testar
        class DummySource(BaseSource):
            def extract(self, **kw): return pd.DataFrame()
            def clean(self, raw): return raw
            def load(self, clean, lookups): return "0 registros"

        source = DummySource()
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = source.validate(df)
        assert result.equals(df)

    def test_validate_empty_dataframe(self):
        """DataFrame vazio é aceito sem validação."""
        from pipeline.base import BaseSource

        class DummySource(BaseSource):
            schema = ConabProducaoSchema
            def extract(self, **kw): return pd.DataFrame()
            def clean(self, raw): return raw
            def load(self, clean, lookups): return "0 registros"

        source = DummySource()
        result = source.validate(pd.DataFrame())
        assert result.empty

    def test_validate_with_schema_passes(self):
        """DataFrame válido passa validação via BaseSource."""
        from pipeline.base import BaseSource

        class DummySource(BaseSource):
            schema = AgrofitSchema
            def extract(self, **kw): return pd.DataFrame()
            def clean(self, raw): return raw
            def load(self, clean, lookups): return "0 registros"

        source = DummySource()
        df = pd.DataFrame({
            "nr_registro": ["123"],
            "marca_comercial": ["PRODUTO X"],
            "cultura": ["soja"],
        })
        result = source.validate(df)
        assert not result.empty

    def test_validate_with_schema_fails(self):
        """DataFrame inválido levanta SchemaErrors via BaseSource."""
        from pipeline.base import BaseSource

        class DummySource(BaseSource):
            schema = AgrofitSchema
            def extract(self, **kw): return pd.DataFrame()
            def clean(self, raw): return raw
            def load(self, clean, lookups): return "0 registros"

        source = DummySource()
        df = pd.DataFrame({
            "nr_registro": ["123"],
            # Missing 'cultura' — required column
        })
        with pytest.raises(pa.errors.SchemaErrors):
            source.validate(df)
