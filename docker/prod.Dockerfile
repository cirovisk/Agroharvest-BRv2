FROM python:3.12-slim

# Evita que o Python gere arquivos .pyc e permite que logs apareçam no console em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

WORKDIR /app

# Instala apenas as dependências mínimas do sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir gunicorn

# Copia o código fonte (em prod, o código fica fixo na imagem)
COPY . .

# Expõe a porta da API
EXPOSE 8000

# Comando para rodar com Gunicorn e workers Uvicorn (Otimizado para produção)
# --workers 4: Ajustar conforme o número de cores da sua instância na Oracle
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "api.main:app", "--bind", "0.0.0.0:8000"]
