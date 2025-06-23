FROM python:3.10-slim

WORKDIR /app

# Установить системные зависимости
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Копировать и установить Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копировать код приложения
COPY app/ ./app/
COPY scripts/ ./scripts/

# Создать директории
RUN mkdir -p /app/data /app/logs

# Установить переменные окружения
ENV PYTHONPATH=/app
ENV FLASK_APP=app.app

EXPOSE 8080

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "4", "--timeout", "120", "app.app:app_prod"]
