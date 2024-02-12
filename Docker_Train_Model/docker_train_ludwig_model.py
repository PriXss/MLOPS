import os
import pandas as pd
from ludwig.api import LudwigModel
import configparser
import boto3
import shutil
import tempfile
import zipfile
from datetime import datetime


class MLFlowTrainer:
    def __init__(self, ludwig_config_path, model_bucket_url, dataset_path, model_name="deep_lstm"):
        self.ludwig_config_path = ludwig_config_path
        self.model_bucket_url = model_bucket_url
        self.dataset_path = dataset_path
        self.model_name = model_name

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
        obj = s3.get_object(Bucket=self.data_bucket_url, Key='data.csv')
        data = pd.read_csv(obj['Body'])

        # Trainieren des Ludwig-Modells
        # Aufteilung der Daten(train_set, test_set, validation_set)
        model = LudwigModel(config=self.ludwig_config_path)
        model.train(dataset=data, split=[0.8, 0.1, 0.1], skip_save_processed_input=True)

        # Speichern des Modells im Bucket
        self.save_model_to_s3(model)

    def save_model_to_s3(self, model):
        s3 = boto3.client('s3')

        # Temporäre Verzeichnisse erstellen
        with tempfile.TemporaryDirectory() as temp_dir_model, tempfile.TemporaryDirectory() as temp_dir_api:
            # Modell speichern
            model_path = os.path.join(temp_dir_model, 'model')
            model.save(model_path)

            # Kopiere den Inhalt von 'api_experiment_run' in das temporäre Verzeichnis
            api_experiment_run_src = os.path.join(os.getcwd(), 'results', 'api_experiment_run')
            if os.path.exists(api_experiment_run_src):
                api_experiment_run_dst = os.path.join(temp_dir_api, 'api_experiment_run')
                shutil.copytree(api_experiment_run_src, api_experiment_run_dst)

            # Verzeichnisse in Zip-Dateien komprimieren
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            model_zip_file_name = f"trained_model_{timestamp}.zip"
            api_zip_file_name = f"api_experiment_run_{timestamp}.zip"

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
    ludwig_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ludwig_MLCore.yaml")
    model_name = "Test_Model"
    dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/data.csv")
    model_bucket_url = "models"

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = MLFlowTrainer(ludwig_config_path, model_bucket_url, dataset_path, model_name)
    trainer.train_model()
