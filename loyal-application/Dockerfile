FROM python:3.9-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости для Supervisor
RUN apt-get update && apt-get install -y supervisor && rm -rf /var/lib/apt/lists/*

# Копируем объединённый requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложений и конфигурации
COPY app.py .
COPY webhook.py .
COPY gunicorn_config.py .
COPY supervisord.conf .
COPY .env .

# Создаём директорию для логов
RUN mkdir -p /app/logs

# Открываем порт 5002
EXPOSE 5002

# Запускаем Supervisor
CMD ["supervisord", "-c", "/app/supervisord.conf"]