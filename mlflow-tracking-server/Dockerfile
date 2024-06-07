FROM python:3.11

# Setze das Arbeitsverzeichnis im Container
WORKDIR /app

COPY .env .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere die Dateien in das Arbeitsverzeichnis im Container
COPY trackingfiles_download.py .
COPY cron.sh .

# Installiere die Python-Abhängigkeiten und Cron
RUN apt-get update && apt-get install -y cron

# Setze Berechtigungen für das Cron-Script und führe es aus
RUN chmod +x /app/cron.sh

# Füge das Cron-Job zur Crontab hinzu
RUN crontab /app/cron.sh

# Starte den Cron-Dienst und den MLflow-Server
CMD touch /var/log/cron.log && cron && tail -f /var/log/cron.log & mlflow server --host 0.0.0.0
