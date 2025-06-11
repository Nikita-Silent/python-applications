FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get upgrade -y

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY webhook.py . 
COPY app.py . 
COPY .env .

RUN mkdir -p /app/logs

EXPOSE 5002 5003