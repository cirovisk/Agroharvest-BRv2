FROM python:3.12-slim

WORKDIR /app

# Instala dependências básicas do sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala dependências python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código fonte
COPY . .

# Variáveis de ambiente DuckDB
ENV PYTHONPATH=/app/src
ENV STORAGE_PATH=/app/data/storage

# Comando padrão
CMD ["python", "src/main.py"]
