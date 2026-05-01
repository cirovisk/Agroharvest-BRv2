#!/bin/bash
# Script para baixar o driver DuckDB oficial (MotherDuck) para o Metabase
PLUGINS_DIR="./plugins"
URL="https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/latest/download/duckdb.metabase-driver.jar"

if [ ! -d "$PLUGINS_DIR" ]; then
  mkdir -p "$PLUGINS_DIR"
fi

echo "Baixando driver DuckDB oficial..."
curl -L "$URL" -o "$PLUGINS_DIR/duckdb.metabase-driver.jar"

if [ -f "$PLUGINS_DIR/duckdb.metabase-driver.jar" ]; then
  SIZE=$(du -h "$PLUGINS_DIR/duckdb.metabase-driver.jar" | cut -f1)
  echo "Sucesso! Driver baixado ($SIZE)."
else
  echo "Erro ao baixar o driver."
fi
