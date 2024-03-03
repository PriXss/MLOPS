# Verwende ein Basisimage, das die erforderlichen Tools enthält (z. B. Python)
FROM python:3.9

# Setze das Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere die Dateien in das Arbeitsverzeichnis im Container
COPY src/trackingfiles_download.py /app/trackingfiles_download.py
COPY requirements.txt /app/requirements.txt
COPY cron.sh /app/cron.sh
COPY mlflow.sh /app/mlflow.sh

# Installiere die Python-Abhängigkeiten
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y cron

# Setze Berechtigungen für die Skripte und führe sie aus
RUN chmod +x /app/cron.sh /app/mlflow.sh

# Füge das Cron-Job zur Crontab hinzu
RUN crontab /app/cron.sh

# Starte den Cron-Dienst und den MLflow-Server im Hintergrund
CMD ["/bin/bash", "-c", "/app/cron.sh & /app/mlflow.sh"]
