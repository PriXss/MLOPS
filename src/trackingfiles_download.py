import os
import boto3
import zipfile
import yaml

def download_mlflow_runs(bucket_name, local_directory, access_key_id, secret_access_key, endpoint_url):
    # Verbindung zum S3-Client herstellen
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      endpoint_url=endpoint_url)

    # Liste aller Objekte im Bucket abrufen
    objects = s3.list_objects_v2(Bucket=bucket_name)['Contents']

    # Lokales Verzeichnis für mlruns erstellen, falls es nicht existiert
    mlruns_dir = os.path.join(local_directory, 'mlruns')
    os.makedirs(mlruns_dir, exist_ok=True)

    # Durch jedes Objekt im Bucket iterieren
    for obj in objects:
        # Objekt-Key abrufen
        obj_key = obj['Key']

        # Datei herunterladen
        local_file_path = os.path.join(mlruns_dir, obj_key)
        s3.download_file(bucket_name, obj_key, local_file_path)
        print(f"Downloaded {obj_key} to {local_file_path}")

        # Überprüfen, ob die heruntergeladene Datei eine Zip-Datei ist
        if obj_key.endswith('.zip'):
            # Zip-Datei entpacken
            with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.join(mlruns_dir, os.path.splitext(obj_key)[0]))
            # Heruntergeladene Zip-Datei löschen, nachdem sie entpackt wurde
            os.remove(local_file_path)
            print(f"Extracted and removed {obj_key}")

            # Pfade für den entpackten Run und die meta.yaml-Datei erstellen
            run_dir = os.path.join(mlruns_dir, os.path.splitext(obj_key)[0])
            meta_yaml_path = os.path.join(run_dir, 'meta.yaml')

            # Den artifact_uri in der meta.yaml-Datei aktualisieren
            with open(meta_yaml_path, 'r') as meta_file:
                meta_data = yaml.safe_load(meta_file)
            meta_data['artifact_uri'] = os.path.join(local_directory, 'mlruns', meta_data['run_id'], 'artifacts')
            with open(meta_yaml_path, 'w') as meta_file:
                yaml.safe_dump(meta_data, meta_file)

            print(f"Updated artifact_uri in {meta_yaml_path}")

if __name__ == "__main__":
    # Bucket-Name und lokales Verzeichnis festlegen
    bucket_name = "mlflowtracking"
    local_directory = os.path.join(os.getcwd(), '..')  # Eine Ebene höher im Ordner MLOPS

    # Zugangsdaten
    access_key_id = "test"
    secret_access_key = "testpassword"
    endpoint_url = "http://85.215.53.91:9000"

    # MLflow-Runs herunterladen und Zip-Dateien entpacken
    download_mlflow_runs(bucket_name, local_directory, access_key_id, secret_access_key, endpoint_url)
