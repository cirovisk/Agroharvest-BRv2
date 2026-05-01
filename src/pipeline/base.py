"""
Contrato base para todos os pipelines de fonte de dados.
Cada source DEVE implementar extract(), clean() e load().
"""
import os
import time
import logging
from abc import ABC, abstractmethod
from pipeline.parquet_utils import save_as_parquet


class BaseSource(ABC):
    """Interface que toda fonte de dados deve seguir."""

    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

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

    def run(self, lookups: dict, **kwargs) -> str:
        """Executa o pipeline completo: extract → clean → load."""
        self.log.info("Iniciando pipeline...")
        raw = self.extract(**kwargs)
        clean = self.clean(raw)
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
