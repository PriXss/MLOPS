# Verwende ein Basisimage, das die erforderlichen Tools enthält (z. B. Python)
FROM python:3.9-slim

# Setze das Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere die Dateien in das Arbeitsverzeichnis im Container
COPY src/trackingfiles_download.py /app/trackingfiles_download.py
COPY requirements.txt /app/requirements.txt
COPY cron.sh /app/cron.sh

# Installiere die Python-Abhängigkeiten
RUN pip install --no-cache-dir -r requirements.txt

# Setze Berechtigungen für das Cron-Script und führe es aus
RUN chmod +x /app/cron.sh
RUN crontab /app/cron.sh

# Starte den Cron-Service und den MLflow-Server
CMD ["cron", "&&", "mlflow", "server", "--port", "8080", "--backend-store-uri", "./mlruns"]
