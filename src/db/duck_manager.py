import logging
import os

import duckdb
import pandas as pd

log = logging.getLogger(__name__)


class DuckManager:
    """
    DuckDB manager for the Lakehouse architecture.
    Automatically maps Parquet files to SQL views and stores dimensions.
    """

    def __init__(self, storage_path=None, db_file=None):
        self.storage_path = storage_path or os.getenv("STORAGE_PATH", "data/storage")
        db_file = db_file or os.getenv("DUCKDB_FILE", os.path.join(self.storage_path, "cultivares.duckdb"))
        os.makedirs(self.storage_path, exist_ok=True)
        self.conn = duckdb.connect(database=db_file)
        self.refresh_views()

    def refresh_views(self):
        """
        Scan the storage directory and create/update DuckDB views.
        This makes it possible to query Parquet files as real SQL tables.
        """
        if not os.path.exists(self.storage_path):
            log.warning(f"Storage directory not found: {self.storage_path}")
            return

        # List subfolders that represent our "tables"
        tables = [d for d in os.listdir(self.storage_path) if os.path.isdir(os.path.join(self.storage_path, d))]

        import re

        # Valid SQL identifier: starts with a letter/underscore and contains only alphanumerics/underscores
        safe_sql_identifier = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        for table in tables:
            if not safe_sql_identifier.match(table):
                log.warning(f"Suspicious table/directory name ignored to avoid SQL injection: '{table}'")
                continue

            # Pattern for reading every Parquet file in the table folder
            parquet_pattern = os.path.join(self.storage_path, table, "*.parquet")

            # Check for files before creating the view
            files = [f for f in os.listdir(os.path.join(self.storage_path, table)) if f.endswith(".parquet")]
            if not files:
                continue

            try:
                # DuckDB reads Parquet files very efficiently
                self.conn.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{parquet_pattern}')")
                log.info(f"DuckDB View: '{table}' synchronized with Parquet files in {table}/")
            except Exception as e:
                log.error(f"Failed to map table {table}: {e}")

    def execute_query(self, sql: str, params: tuple | list | dict | None = None) -> pd.DataFrame:
        """Execute parameterized SQL and return the result as a Pandas DataFrame."""
        try:
            return self.conn.execute(sql, params).df()
        except Exception as e:
            log.error(f"DuckDB query error: {e}")
            raise


class LazyDuckManagerProxy:
    """
    Lazy proxy for DuckManager.
    Avoids physical I/O (directory creation, file connections)
    when the 'db.duck_manager' module is imported.
    """

    def __init__(self):
        # Define the internal attribute with super() to bypass recursive __setattr__
        super().__setattr__("_instance", None)

    def _get_instance(self) -> DuckManager:
        if self._instance is None:
            super().__setattr__("_instance", DuckManager())
        return self._instance

    def __getattr__(self, name):
        return getattr(self._get_instance(), name)

    def __setattr__(self, name, value):
        setattr(self._get_instance(), name, value)


# Single instance (lazy singleton loaded on demand)
duck_db = LazyDuckManagerProxy()
