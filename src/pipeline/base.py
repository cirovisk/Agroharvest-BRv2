"""
Base contract for all data-source pipelines.
Each source MUST implement extract(), clean(), and load().

Inherited features:
  - self.http: resilient HTTP client with retries and exponential backoff
  - self.validate(): Pandera schema validation after clean()
  - self.save_parquet(): helper to save DataFrames as Parquet
"""

import logging
import os
import time
from abc import ABC, abstractmethod

from pandera.errors import SchemaErrors

from pipeline.http_client import ResilientHTTPClient
from pipeline.parquet_utils import save_as_parquet


class BaseSource(ABC):
    """Interface every data source must follow."""

    # Subclasses may override this to customize the validation schema.
    # If None, validation is skipped (pass-through).
    schema = None

    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self._http: ResilientHTTPClient | None = None

    @property
    def http(self) -> ResilientHTTPClient:
        """
        Resilient HTTP client, created lazily.
        Subclasses may override _http_config() to customize it.
        """
        if self._http is None:
            self._http = ResilientHTTPClient(**self._http_config())
        return self._http

    def _http_config(self) -> dict:
        """
        HTTP client configuration. Override to customize per source.
        Example: Agrofit needs timeout=300, MAPA needs ssl_fallback=True.
        """
        return {}

    def save_parquet(self, df, table_name, **kwargs):
        """Helper to save DataFrames as Parquet with Brotli."""
        return save_as_parquet(df, table_name, **kwargs)

    @abstractmethod
    def extract(self, **kwargs):
        """Extract raw data from the source. Returns a DataFrame, dict, or generator."""
        ...

    @abstractmethod
    def clean(self, raw_data):
        """Clean and standardize raw data."""
        ...

    @abstractmethod
    def load(self, clean_data, lookups: dict):
        """Load clean data into the database through upsert."""
        ...

    def validate(self, df, schema=None):
        """
        Validate a DataFrame against a Pandera schema.

        Args:
            df: DataFrame to validate
            schema: Pandera schema (if None, uses self.schema)

        Returns:
            Validated DataFrame (potentially with coerced types)

        Raises:
            pandera.errors.SchemaErrors: If validation fails
        """
        effective_schema = schema or self.schema
        if effective_schema is None or df.empty:
            return df

        try:
            validated = effective_schema.validate(df, lazy=True)
            self.log.info(f"✓ Schema '{effective_schema.name or 'unnamed'}' validado: {len(df)} linha(s) OK.")
            return validated
        except SchemaErrors as e:
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

        # Schema validation (if defined)
        if isinstance(clean, dict):
            # Sources with multiple DataFrames (CONAB, SIGEF)
            for key, df in clean.items():
                if hasattr(self, "_get_schema_for_key"):
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
            return True
        file_age_days = (time.time() - os.path.getmtime(path)) / (24 * 3600)
        return file_age_days > threshold_days

    def _archive_file(self, local_path: str, data_dir: str):
        """
        Move um arquivo local antigo para uma subpasta de archive com timestamp.
        """
        if os.path.exists(local_path):
            import shutil
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = os.path.join(data_dir, "archive")
            os.makedirs(archive_dir, exist_ok=True)
            filename = os.path.basename(local_path)
            name, ext = os.path.splitext(filename)
            archive_path = os.path.join(archive_dir, f"{name}__{timestamp}{ext}")
            shutil.move(local_path, archive_path)
            self.log.info(f"Arquivo antigo arquivado: {os.path.basename(archive_path)}")
