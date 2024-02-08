# Verwenden des offiziellen Python 3.8-Basisimages
FROM python:3.8

# Arbeitsverzeichnis im Container festlegen
WORKDIR /app

# Kopieren der Anwendungscodes in das Arbeitsverzeichnis im Container
COPY . .

# Kopieren der Konfigurationsdatei in das Arbeitsverzeichnis im Container
COPY config.ini .

# Installation der erforderlichen Pakete
RUN pip install -r requirements.txt

# Ausführen des MLFlowTrainer beim Containerstart
CMD ["python", "MLFlowTrainer.py"]
