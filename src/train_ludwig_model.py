import os
from ludwig.api import LudwigModel


class LudwigModelTrainer:
    def __init__(self, ludwig_config_path, dataset_path, model_name="deep_lstm"):
        self.ludwig_config_path = ludwig_config_path
        self.dataset_path = dataset_path
        self.model_name = model_name

        # Pfadeinstellungen für Models
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.models_dir = os.path.abspath("../models")
        self.results_dir = os.path.join(self.script_dir, "../models", model_name)

    def train_model(self):
        # Trainiert das Ludwig-Modell
        model = LudwigModel(config=self.ludwig_config_path)
        # Aufteilung der Daten(train_set, test_set, validation_set)
        training_stats = model.train(dataset=self.dataset_path, split=[0.8, 0.1, 0.1])

        # Speichert das Modell
        model_dir = os.path.join(self.models_dir, self.model_name)
        model.save(model_dir)


if __name__ == "__main__":
    # Pfade anpassen
    ludwig_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ludwig_MLCore.yaml")
    dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/data.csv")

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = LudwigModelTrainer(ludwig_config_path, dataset_path)
    trainer.train_model()
