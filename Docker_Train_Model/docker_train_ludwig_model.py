import os
import pandas as pd
from ludwig.api import LudwigModel
import configparser
import boto3
from io import StringIO

class MLFlowTrainer:
    def __init__(self, ludwig_config_path, model_bucket_url, dataset_path, model_name="deep_lstm"):
        self.ludwig_config_path = ludwig_config_path
        self.model_bucket_url = model_bucket_url
        self.dataset_path = dataset_path
        self.model_name = model_name

        # Setzen der Bucket-URLs
        self.data_bucket_url = "s3://data"
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
        model.train(dataset=data, split=[0.8, 0.1, 0.1])

        # Speichern des Modells im Bucket
        self.save_model_to_s3(model)

    def save_model_to_s3(self, model):
        s3 = boto3.client('s3')
        with open('model.pkl', 'wb') as f:
            model.save(f.name)
            s3.upload_file(f.name, self.model_bucket_url, 'trained_model.pkl')


if __name__ == "__main__":
    # Pfade anpassen
    ludwig_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ludwig_MLCore.yaml")
    model_name = "Test_Model"
    dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/data.csv")
    model_bucket_url = "s3://data"

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = MLFlowTrainer(ludwig_config_path, model_bucket_url, dataset_path, model_name)
    trainer.train_model()
