import os
import pandas as pd
from ludwig.api import LudwigModel
import boto3
import shutil
import tempfile
import zipfile
from datetime import datetime
import yaml

class MLFlowTrainer:
    def __init__(self, model_bucket_url, model_name="", ludwig_config_file_name="", data_file_name= ""):
        self.ludwig_config_bucket_url = "modelconfigs"
        self.model_bucket_url = model_bucket_url
        self.model_name = model_name
        self.ludwig_config_file_name = ludwig_config_file_name
        self.data_file_name = data_file_name

        # Setzen der Bucket-URLs
        self.data_bucket_url = "data"
        self.access_key_id = "test"
        self.secret_access_key = "testpassword"
        self.endpoint_url = "http://85.215.53.91:9000"

        # Setzen der Umgebungsvariablen für den Zugriff auf Buckets
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint_url

    def train_model(self):
        # Laden der Daten aus dem Bucket
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.data_bucket_url, Key=self.data_file_name)
        data = pd.read_csv(obj['Body'])

        # Temporäre Datei für die Ludwig-Konfigurationsdatei erstellen
        temp_ludwig_config_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            # Ludwig-Konfigurationsdatei aus dem Bucket herunterladen und lokal speichern
            s3.download_fileobj(Bucket=self.ludwig_config_bucket_url, Key=self.ludwig_config_file_name,
                                Fileobj=temp_ludwig_config_file)

            temp_ludwig_config_file.close()

            # Extrahiere den Modellnamen aus der Ludwig-Konfigurationsdatei
            model_name = self.extract_model_name(temp_ludwig_config_file.name)

            # Ludwig-Modell trainieren
            model = LudwigModel(config=temp_ludwig_config_file.name)
            model.train(dataset=data, split=[0.8, 0.1, 0.1], skip_save_processed_input=True)

            # Modell speichern und hochladen
            self.save_model_to_s3(model, model_name)
        finally:
            os.unlink(temp_ludwig_config_file.name)

    # Name des Models aus ludwig-config auslesen (abhängig vom dort definierten model-type)
    def extract_model_name(self, yaml_file_path):
        with open(yaml_file_path, 'r') as file:
            yaml_content = yaml.safe_load(file)
            model_name = None
            if 'model' in yaml_content and 'type' in yaml_content['model']:
                model_name = yaml_content['model']['type']
            return model_name

    def save_model_to_s3(self, model, model_name):
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
            model_zip_file_name = f"trained_model_{model_name}_{timestamp}.zip"
            api_zip_file_name = f"api_experiment_run_{model_name}_{timestamp}.zip"

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


if __name__ == "__main__":
    # Pfade anpassen
    ludwig_config_file_name = "ludwig_MLCore.yaml"  # Name der Ludwig-Konfigurationsdatei
    model_bucket_url = "models"
    data_file = "data.csv"

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = MLFlowTrainer(model_bucket_url, ludwig_config_file_name=ludwig_config_file_name,
                            data_file_name=data_file)
    trainer.train_model()
