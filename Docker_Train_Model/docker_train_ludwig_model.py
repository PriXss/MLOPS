import os
import mlflow
import pandas as pd
from ludwig.api import LudwigModel
import configparser


class MLFlowTrainer:
    def __init__(self, ludwig_config_path, model_name=""):
        self.ludwig_config_path = ludwig_config_path
        self.model_name = model_name

        # Laden der Bucket-Informationen aus der Konfigurationsdatei
        self.config = configparser.ConfigParser()
        self.config.read('Dataconfig')

        # Setzen der Bucket-URLs
        self.data_bucket_url = self.config['remote "minio"']['url']
        self.access_key_id = self.config['remote "minio"']['access_key_id']
        self.secret_access_key = self.config['remote "minio"']['secret_access_key']
        self.endpoint_url = self.config['remote "minio"']['endpointurl']

        # Setzen der Umgebungsvariablen für den Zugriff auf Buckets
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint_url

        #Lade die modelConfig
        self.config = configparser.ConfigParser()
        self.config.read('config')
        self.model_bucket_url = self.config['remote "minio"']['url']

    def train_model(self):
        # Laden der Daten aus dem Bucket
        data = pd.read_csv(self.data_bucket_url)

        # Trainieren des Ludwig-Modells
        # Aufteilung der Daten(train_set, test_set, validation_set)
        model = LudwigModel(config=self.ludwig_config_path)
        model.train(dataset=data, split=[0.8, 0.1, 0.1])

        # Speichern des Modells im Bucket
        model.save(self.model_bucket_url)

        # Logging des Modells mit MLflow
        with mlflow.start_run():
            mlflow.log_artifacts(self.data_bucket_url, artifact_path=self.model_name)
            mlflow.log_params({"ludwig_config_path": self.ludwig_config_path})



if __name__ == "__main__":
    # Pfade anpassen
    ludwig_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ludwig_MLCore.yaml")
    model_name = "Test_Model"
    #dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/transformierte_datei.csv")

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = MLFlowTrainer(ludwig_config_path, model_name)
    trainer.train_model()