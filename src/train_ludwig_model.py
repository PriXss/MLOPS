import mlflow
import os
from ludwig.api import LudwigModel


class MLFlowTrainer:
    def __init__(self, ludwig_config_path, dataset_path, model_name="Test_Kurs"):
        self.ludwig_config_path = ludwig_config_path
        self.dataset_path = dataset_path
        self.model_name = model_name

        # Pfadeinstellungen für Models
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.models_dir = os.path.abspath("../models")
        self.results_dir = os.path.join(self.script_dir, "../models", model_name)

        # Setzt das mlflow-Tracking-Verzeichnis für das gesamte Projekt
        self.project_tracking_dir = os.path.abspath("../mlflow_tracking")
        mlflow.set_tracking_uri(f"file://{self.project_tracking_dir}")

    def train_model(self):
        # Trainiert das Ludwig-Modell
        model = LudwigModel(config=self.ludwig_config_path)
        # Aufteilung der Daten(train_set, test_set, validation_set)
        model.train(dataset=self.dataset_path, split=[0.8, 0.1, 0.1])

        # Speichert das Modell
        model_dir = os.path.join(self.models_dir, self.model_name)
        model.save(model_dir)

        # Model Logging mit MLflow
        with mlflow.start_run():
            mlflow.log_artifacts(model_dir, artifact_path=self.model_name)
            mlflow.log_params({"ludwig_config_path": self.ludwig_config_path})
            #mlflow.log_metric()


if __name__ == "__main__":
    # Pfade anpassen
    ludwig_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ludwig_MLCore.yaml")
    dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/transformierte_datei.csv")

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = MLFlowTrainer(ludwig_config_path, dataset_path)
    trainer.train_model()
