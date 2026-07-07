import logging
import os

import pandas as pd

log = logging.getLogger(__name__)


def save_as_parquet(df: pd.DataFrame, table_name: str, base_path: str = "data/storage", file_suffix: str = ""):
    """
    Save a DataFrame in Parquet format using Brotli compression.
    Structure: base_path/table_name/data{file_suffix}.parquet
    """
    if df.empty:
        log.warning(f"Empty DataFrame for {table_name}. Nothing will be saved.")
        return None

    # Ensure the table directory exists
    table_path = os.path.join(base_path, table_name)
    os.makedirs(table_path, exist_ok=True)

    # File name (timestamps or partitions could be used here in the future)
    suffix_str = f"_{file_suffix}" if file_suffix else ""
    file_path = os.path.join(table_path, f"data{suffix_str}.parquet")

    try:
        # Save with Brotli (requires pyarrow or fastparquet)
        df.to_parquet(file_path, compression="brotli", index=False)
        log.info(f"Data saved successfully: {file_path} ({len(df)} rows)")
        return file_path
    except Exception as e:
        log.error(f"Error saving Parquet for {table_name}: {e}")
        raise
