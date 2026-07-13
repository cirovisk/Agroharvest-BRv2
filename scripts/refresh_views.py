#!/usr/bin/env python3
"""Refresh DuckDB views that point to Parquet fact folders."""

from __future__ import annotations

from db.duck_manager import DuckManager


def main() -> int:
    manager = DuckManager()
    manager.refresh_views()
    print("DuckDB views refreshed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
