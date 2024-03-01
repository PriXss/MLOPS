import os
import pandas as pd
from ludwig.api import LudwigModel
import boto3
import shutil
import tempfile
import zipfile
from datetime import datetime
import yaml
import mlflow

class MLFlowTrainer:
    def __init__(self, model_bucket_url, model_name="", ludwig_config_file_name="", data_file_name=""):
        self.model_bucket_url = model_bucket_url
        self.model_name = model_name
        self.ludwig_config_file_name = ludwig_config_file_name
        self.data_file_name = data_file_name

        # Setzen der Bucket-URLs
        self.mlflow_bucket_url = "mlflowtracking"
        self.data_bucket_url = "data"
        self.model_configs_bucket_url = "modelconfigs"
        self.access_key_id = "test"
        self.secret_access_key = "testpassword"
        self.endpoint_url = "http://85.215.53.91:9000"

        # Setzen der Umgebungsvariablen für den Zugriff auf Buckets
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint_url

    def get_data_name_from_bucket(self):
        # Verbindung zum S3-Client herstellen
        s3 = boto3.client('s3')

        # Datei mit der Variable data_name aus dem Bucket modelconfigs herunterladen
        obj = s3.get_object(Bucket=self.model_configs_bucket_url, Key="data_name.txt")
        data_name = obj['Body'].read().decode('utf-8').strip()

        return data_name

    def upload_directory_to_s3(self, local_path, bucket, s3_path):
        s3_client = boto3.client('s3')
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_path)
                s3_file = os.path.join(s3_path, relative_path)
                s3_client.upload_file(local_file, bucket, s3_file)

    def train_model(self):
        # Extrahieren der Variable data_name aus der Textdatei im Bucket
        data_name = self.get_data_name_from_bucket()

        # Pfade anpassen
        data_file = f"data_{data_name}.csv"

        # Laden der Daten aus dem Bucket
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.data_bucket_url, Key=data_file)
        data = pd.read_csv(obj['Body'])

        # Starten des MLflow-Laufs
        with mlflow.start_run() as run:
            self.run_id = run.info.run_id  # Run-ID speichern


            try:
                # Temporäre Datei für die Ludwig-Konfigurationsdatei erstellen
                temp_ludwig_config_file = tempfile.NamedTemporaryFile(delete=False)
                try:
                    # Ludwig-Konfigurationsdatei aus dem Bucket herunterladen und lokal speichern
                    s3.download_fileobj(Bucket=self.model_configs_bucket_url, Key=self.ludwig_config_file_name,
                                        Fileobj=temp_ludwig_config_file)

                    temp_ludwig_config_file.close()

                    # Extrahiere den Modellnamen aus der Ludwig-Konfigurationsdatei
                    model_name = self.extract_model_name(temp_ludwig_config_file.name)

                    # Namensgebung ML Run
                    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    mlflow.set_tag('mlflow.runName', f'{data_name}_{model_name}_{timestamp}')

                    # Ludwig-Modell trainieren
                    ludwig_model = LudwigModel(config=temp_ludwig_config_file.name)
                    train_stats, _, _ = ludwig_model.train(dataset=data, split=[0.8, 0.1, 0.1],
                                                           skip_save_processed_input=True)

                    # Loggen der Parameter
                    self.log_params(data_name, data_file)

                    # Loggen der Metriken
                    self.log_metrics(train_stats)

                    # Speichern von Artefakten
                    with tempfile.TemporaryDirectory() as temp_dir_model:
                        # Modell speichern
                        model_path = os.path.join(temp_dir_model, model_name)
                        ludwig_model.save(model_path)
                        mlflow.log_artifact(model_path, artifact_path='')

                    # Speichern von Artefakten
                    self.save_model_to_s3(ludwig_model, model_name, data_name)

                    # Hochladen der meta.yaml-Datei in den S3-Bucket modelconfigs
                    local_path = os.path.join(os.getcwd(), 'mlruns', '0', self.run_id)
                    self.upload_meta_yaml_to_s3(local_path)

                finally:
                    os.unlink(temp_ludwig_config_file.name)

            finally:
                # MLflow-Lauf beenden
                mlflow.end_run()

                # Den Ordner des aktuellen MLflow-Laufs komprimieren und als Zip-Datei hochladen
                zip_file_name = f"{self.run_id}.zip"
                zip_file_path = os.path.join(os.getcwd(), 'mlruns', '0', zip_file_name)
                shutil.make_archive(os.path.join(os.getcwd(), 'mlruns', '0', self.run_id), 'zip', local_path)
                s3.upload_file(zip_file_path, "mlflowtracking", zip_file_name)

                # Lokale Runs nach dem Upload löschen
                shutil.rmtree(os.path.join(os.getcwd(), 'mlruns'))

    def upload_meta_yaml_to_s3(self, local_path):
        meta_yaml_path = os.path.join(local_path, "..", "meta.yaml")
        if os.path.exists(meta_yaml_path):
            # Lese den Inhalt der meta.yaml-Datei
            with open(meta_yaml_path, 'r') as file:
                meta_yaml_content = yaml.safe_load(file)

            # Passe den artifact_location-Pfad relativ zum aktuellen Verzeichnis an
            meta_yaml_content['artifact_location'] = './mlruns/0'

            # Speichere den angepassten Inhalt zurück in die meta.yaml-Datei
            with open(meta_yaml_path, 'w') as file:
                yaml.dump(meta_yaml_content, file)

            # Lade die angepasste meta.yaml-Datei in den S3-Bucket hoch
            s3_client = boto3.client('s3')
            s3_client.upload_file(meta_yaml_path, self.model_configs_bucket_url, "meta.yaml")

    def extract_model_name(self, yaml_file_path):
        with open(yaml_file_path, 'r') as file:
            yaml_content = yaml.safe_load(file)
            model_name = None
            if 'model' in yaml_content and 'type' in yaml_content['model']:
                model_name = yaml_content['model']['type']
            return model_name

    def save_model_to_s3(self, model, model_name, data_name):
        s3 = boto3.client('s3')

        # Temporäre Verzeichnisse erstellen
        with tempfile.TemporaryDirectory() as temp_dir_model, tempfile.TemporaryDirectory() as temp_dir_api:
            # Modell speichern
            model_path = os.path.join(temp_dir_model, 'model')
            model.save(model_path)

            # Kopiere den Inhalt von 'api_experiment_run' in das temporäre Verzeichnis
            api_experiment_run_src = os.path.join(os.getcwd(), '../src/results', 'api_experiment_run')
            if os.path.exists(api_experiment_run_src):
                api_experiment_run_dst = os.path.join(temp_dir_api, 'api_experiment_run')
                shutil.copytree(api_experiment_run_src, api_experiment_run_dst)

            # Verzeichnisse in Zip-Dateien komprimieren
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            model_zip_file_name = f"trained_model_{data_name}_{model_name}_{timestamp}.zip"
            api_zip_file_name = f"api_experiment_run_{data_name}_{model_name}_{timestamp}.zip"

            model_zip_file_path = os.path.join(temp_dir_model, model_zip_file_name)
            api_zip_file_path = os.path.join(temp_dir_api, api_zip_file_name)

            for folder, zip_file_path in [(model_path, model_zip_file_path),
                                          (api_experiment_run_dst, api_zip_file_path)]:
                if folder:
                    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
                        for root, dirs, files in os.walk(folder):
                            for file in files:
                                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), folder))

            # Zip-Dateien in die S3-Buckets hochladen
            s3.upload_file(model_zip_file_path, self.model_bucket_url, model_zip_file_name)
            if api_experiment_run_dst:
                s3.upload_file(api_zip_file_path, "mlcoreoutputrun", api_zip_file_name)

        shutil.rmtree(os.path.join(os.getcwd(), '../src/results'))


    def log_params(self, data_name, data_file):
        mlflow.log_param("data_name", data_name)
        mlflow.log_param("ludwig_config_file_name", self.ludwig_config_file_name)
        mlflow.log_param("data_file_name", data_file)

    def log_metrics(self, train_stats):
        phases = ['test', 'training', 'validation']
        for phase in phases:
            section = train_stats[phase]["Schluss"]
            for metric_name, values in section.items():
                for idx, value in enumerate(values):
                    mlflow.log_metric(f"{phase}_{metric_name}", value)

if __name__ == "__main__":
    model_bucket_url = "models"
    ludwig_config_file_name = "ludwig_MLCore.yaml"

    trainer = MLFlowTrainer(model_bucket_url, ludwig_config_file_name=ludwig_config_file_name)
    trainer.train_model()
