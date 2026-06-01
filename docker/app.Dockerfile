FROM python:3.12-slim

WORKDIR /app

# Instala dependências básicas do sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala dependências python
COPY docker/requirements.txt docker/requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

# Cria um grupo e um usuário não-root (UID/GID 1000 para alinhar com o host em volumes montados)
RUN groupadd -g 1000 nonroot && \
    useradd -u 1000 -g nonroot -d /app -s /sbin/nologin nonroot

# Copia o código fonte
COPY . .

# Garante permissões adequadas no diretório de trabalho
RUN mkdir -p /app/data && chown -R nonroot:nonroot /app

# Variáveis de ambiente DuckDB
ENV PYTHONPATH=/app/src
ENV STORAGE_PATH=/app/data/storage

USER nonroot

# Comando padrão
CMD ["python", "src/main.py"]
