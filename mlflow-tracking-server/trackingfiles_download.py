import os
import sys
import boto3
import zipfile
import yaml
import logging

# Konfiguriere das Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Füge einen StreamHandler hinzu, um Protokolle an die Konsole zu senden
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# Füge einen Handler für die Datei cron.log hinzu
file_handler = logging.FileHandler('cron.log')
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

def download_mlflow_runs(mlflow_bucket_name, modelconfigs_bucket_name, local_directory, s3_client):
    logger.info("Starte den Download von MLflow-Runs...")

    # Liste aller Objekte im MLflow-Bucket abrufen
    mlflow_objects = s3_client.list_objects_v2(Bucket=mlflow_bucket_name)['Contents']

    logger.info("Ludwig-Konfigurationsdatei und Modellnamen extrahieren...")
    # Ludwig-Konfigurationsdatei und Modellname extrahieren
    ludwig_config_file_name = "ludwig_MLCore.yaml"
    # model_name = extract_model_name_from_s3(modelconfigs_bucket_name, ludwig_config_file_name, s3_client)

    # Lokales Verzeichnis für mlruns erstellen, falls es nicht existiert
    mlruns_dir = os.path.join(local_directory, 'mlruns', '0')
    os.makedirs(mlruns_dir, exist_ok=True)

    logger.info(f"Local directory: {mlruns_dir}")

    # Durch jedes Objekt im MLflow-Bucket iterieren
    for obj in mlflow_objects:
        # Objekt-Key abrufen
        obj_key = obj['Key']
        logger.info(f"Datei herunterladen: {obj_key}")

        # Datei herunterladen
        local_file_path = os.path.join(mlruns_dir, obj_key)
        s3_client.download_file(mlflow_bucket_name, obj_key, local_file_path)
        logger.info(f"Datei heruntergeladen und gespeichert unter: {local_file_path}")

        # Überprüfen, ob die heruntergeladene Datei eine Zip-Datei ist
        if obj_key.endswith('.zip'):
            # Zip-Datei entpacken
            with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.join(mlruns_dir, os.path.splitext(obj_key)[0]))
            # Heruntergeladene Zip-Datei löschen, nachdem sie entpackt wurde
            os.remove(local_file_path)

            # Pfade für den entpackten Run und die meta.yaml-Datei erstellen
            run_dir = os.path.join(mlruns_dir, os.path.splitext(obj_key)[0])
            meta_yaml_path = os.path.join(run_dir, 'meta.yaml')

            # Den artifact_uri in der meta.yaml-Datei aktualisieren
            with open(meta_yaml_path, 'r') as meta_file:
                meta_data = yaml.safe_load(meta_file)

            # Aktuellen Pfad des Skripts ermitteln
            script_path = os.path.dirname(os.path.realpath(__file__))
            # Aktuellen Run-ID ermitteln
            run_id = os.path.basename(run_dir)
            # Pfad zur artefakt_uri erstellen
            artifact_uri = os.path.abspath(os.path.join(script_path, 'mlruns', '0', run_id, 'artifacts'))

            # Neuen absoluten Pfad zur artefakt_uri verwenden
            meta_data['artifact_uri'] = f'file://{artifact_uri}'

            with open(meta_yaml_path, 'w') as meta_file:
                yaml.safe_dump(meta_data, meta_file)
            logger.info(f"Artifact URI aktualisiert: {artifact_uri}")

    # Herunterladen der meta.yaml-Datei aus dem "modelconfigs"-Bucket
    modelconfigs_meta_yaml_path = os.path.join(mlruns_dir, 'meta.yaml')
    s3_client.download_file(modelconfigs_bucket_name, 'meta.yaml', modelconfigs_meta_yaml_path)
    logger.info("Meta.yaml-Datei heruntergeladen.")

    logger.info("Download von MLflow-Runs abgeschlossen.")


# Die restlichen Funktionen bleiben unverändert...

if __name__ == "__main__":
    # Konfigurieren des Loggings für die Ausgabe in die Konsole
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Bucket-Namen und lokales Verzeichnis festlegen
    mlflow_bucket_name = "mlflowtracking"
    modelconfigs_bucket_name = "modelconfigs"
    local_directory = '/app'

    # Zugangsdaten
    access_key_id = "test"
    secret_access_key = "testpassword"
    endpoint_url = "http://85.215.53.91:9000"

    # Verbindung zum S3-Client herstellen
    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key_id,
                             aws_secret_access_key=secret_access_key,
                             endpoint_url=endpoint_url)

    # MLflow-Runs herunterladen und Zip-Dateien entpacken
    download_mlflow_runs(mlflow_bucket_name, modelconfigs_bucket_name, local_directory, s3_client)
    logger.info(f"Local directory: {local_directory}")
