# Basisimage
FROM python:3.8

# Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere Python-Skript, Cron-Skript und Requirements in den Container
COPY src/requirements.txt .
COPY src/trackingfiles_download.py .
COPY cron_script.sh .

# Installiere Python-Abhängigkeiten
RUN pip install -r requirements.txt

# Kopiere Cron-Job in das Verzeichnis der Cron-Jobs
COPY cron_script.sh /etc/cron.d/cron_script

# Berechtigungen für den Cron-Job setzen
RUN chmod 0644 /etc/cron.d/cron_script

# Starte den Cron-Dienst
CMD ["cron", "-f"]
