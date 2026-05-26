"""
Contrato base para todos os pipelines de fonte de dados.
Cada source DEVE implementar extract(), clean() e load().

Funcionalidades herdadas:
  - self.http: Cliente HTTP resiliente com retries e backoff exponencial
  - self.validate(): Validação de schema Pandera após o clean()
  - self.save_parquet(): Helper para salvar DataFrames em Parquet
"""
import os
import time
import logging
from abc import ABC, abstractmethod

import pandera as pa

from pipeline.parquet_utils import save_as_parquet
from pipeline.http_client import ResilientHTTPClient


class BaseSource(ABC):
    """Interface que toda fonte de dados deve seguir."""

    # Subclasses podem sobrescrever para customizar o schema de validação.
    # Se None, a validação é pulada (pass-through).
    schema = None

    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self._http: ResilientHTTPClient | None = None

    @property
    def http(self) -> ResilientHTTPClient:
        """
        Cliente HTTP resiliente, criado sob demanda (lazy).
        Subclasses podem sobrescrever _http_config() para customizar.
        """
        if self._http is None:
            self._http = ResilientHTTPClient(**self._http_config())
        return self._http

    def _http_config(self) -> dict:
        """
        Configuração do cliente HTTP. Override para customizar por fonte.
        Ex: Agrofit precisa de timeout=300, MAPA precisa de ssl_fallback=True.
        """
        return {}

    def save_parquet(self, df, table_name, **kwargs):
        """Helper para salvar DataFrames em Parquet com Brotli."""
        return save_as_parquet(df, table_name, **kwargs)

    @abstractmethod
    def extract(self, **kwargs):
        """Extrai dados brutos da fonte. Retorna DataFrame, dict, ou generator."""
        ...

    @abstractmethod
    def clean(self, raw_data):
        """Limpa e padroniza os dados brutos."""
        ...

    @abstractmethod
    def load(self, clean_data, lookups: dict):
        """Carrega dados limpos no banco via upsert."""
        ...

    def validate(self, df, schema=None):
        """
        Valida um DataFrame contra um schema Pandera.

        Args:
            df: DataFrame a validar
            schema: Schema Pandera (se None, usa self.schema)

        Returns:
            DataFrame validado (potencialmente com tipos coercidos)

        Raises:
            pandera.errors.SchemaErrors: Se a validação falhar
        """
        effective_schema = schema or self.schema
        if effective_schema is None or df.empty:
            return df

        try:
            validated = effective_schema.validate(df, lazy=True)
            self.log.info(
                f"✓ Schema '{effective_schema.name or 'unnamed'}' validado: "
                f"{len(df)} linha(s) OK."
            )
            return validated
        except pa.errors.SchemaErrors as e:
            self.log.error(
                f"✗ Validação de schema falhou ({effective_schema.name or 'unnamed'}):\n"
                f"  Falhas encontradas:\n{e.failure_cases.to_string()}"
            )
            raise

    def run(self, lookups: dict, **kwargs) -> str:
        """Executa o pipeline completo: extract → clean → validate → load."""
        self.log.info("Iniciando pipeline...")
        raw = self.extract(**kwargs)
        clean = self.clean(raw)

        # Validação de schema (se definido)
        if isinstance(clean, dict):
            # Sources com múltiplos DataFrames (CONAB, SIGEF)
            for key, df in clean.items():
                if hasattr(self, '_get_schema_for_key'):
                    sub_schema = self._get_schema_for_key(key)
                    if sub_schema is not None:
                        clean[key] = self.validate(df, schema=sub_schema)
        else:
            clean = self.validate(clean)

        result = self.load(clean, lookups)
        self.log.info(f"Pipeline concluído: {result}")
        return result

    def is_file_stale(self, path: str, threshold_days: int = 30) -> bool:
        """
        Verifica se um arquivo local está desatualizado baseado na sua idade.
        """
        if not os.path.exists(path):
            return False
        file_age_days = (time.time() - os.path.getmtime(path)) / (24 * 3600)
        return file_age_days > threshold_days
