#!/usr/bin/env python3
"""Validate DuckDB dimensions and Parquet fact folders."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import duckdb


DIMENSIONS = ["dim_cultura", "dim_municipio", "dim_mantenedor"]
FACTS = [
    "fato_registro_cultivares",
    "fato_producao_pam",
    "fato_risco_zarc",
    "fato_producao_conab",
    "fato_precos_conab_mensal",
    "fato_precos_conab_semanal",
    "fato_agrofit",
    "fato_fertilizantes_estabelecimentos",
    "fato_sigef_producao",
    "fato_sigef_reserva_semente",
    "fato_meteorologia",
    "fato_ndvi_satelite",
]


def count_dimension(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, Any]:
    try:
        rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return {"name": table, "kind": "dimension", "status": "ok" if rows else "empty", "rows": int(rows)}
    except Exception as exc:
        return {"name": table, "kind": "dimension", "status": "missing", "rows": 0, "error": str(exc)}


def count_fact(storage_path: Path, table: str) -> dict[str, Any]:
    table_dir = storage_path / table
    files = sorted(table_dir.glob("*.parquet"))
    size_bytes = sum(file.stat().st_size for file in files)
    if not files:
        return {"name": table, "kind": "fact", "status": "missing", "rows": 0, "files": 0, "size_bytes": 0}

    pattern = str(table_dir / "*.parquet")
    try:
        rows = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{pattern}')").fetchone()[0]
        status = "ok" if rows else "empty"
        return {
            "name": table,
            "kind": "fact",
            "status": status,
            "rows": int(rows),
            "files": len(files),
            "size_bytes": size_bytes,
        }
    except Exception as exc:
        return {
            "name": table,
            "kind": "fact",
            "status": "error",
            "rows": 0,
            "files": len(files),
            "size_bytes": size_bytes,
            "error": str(exc),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage-path", default=os.getenv("STORAGE_PATH", "data/storage"))
    parser.add_argument("--duckdb-file", default=os.getenv("DUCKDB_FILE", "data/storage/cultivares.duckdb"))
    parser.add_argument("--manifest", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    storage_path = Path(args.storage_path)
    db_file = Path(args.duckdb_file)

    results = []
    with duckdb.connect(str(db_file)) as conn:
        for dimension in DIMENSIONS:
            results.append(count_dimension(conn, dimension))

    for fact in FACTS:
        results.append(count_fact(storage_path, fact))

    print("name,kind,status,rows,files,size_bytes")
    for result in results:
        print(
            f"{result['name']},{result['kind']},{result['status']},"
            f"{result.get('rows', 0)},{result.get('files', '')},{result.get('size_bytes', '')}"
        )

    if args.manifest:
        manifest_path = Path(args.manifest)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps({"storage_path": str(storage_path), "tables": results}, indent=2), encoding="utf-8")
        print(f"\nManifest written to {manifest_path}")

    failures = [item for item in results if item["status"] != "ok"]
    if failures:
        print("\nValidation failed:")
        for failure in failures:
            print(f"- {failure['name']}: {failure['status']}")
        return 1

    print("\nValidation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
