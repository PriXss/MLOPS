import os
import pandas as pd
from ludwig.api import LudwigModel


class ModelPredictor:
    def __init__(self, model_dir):
        self.model_dir = model_dir

    def load_model(self):
        # Lade das trainierte Modell
        self.model = LudwigModel.load(self.model_dir)

    def predict(self, data):
        # Mache Vorhersagen auf den Daten
        predictions = self.model.predict(data)
        return predictions


if __name__ == "__main__":
    # Pfade anpassen
    model_dir = "Pfad_zu_deinem_trainierten_Modell"
    data_path = "Pfad_zu_deinen_neuen_Daten.csv"

    # Lade den trainierten Modell
    predictor = ModelPredictor(model_dir)
    predictor.load_model()

    # Lade neue Daten
    new_data = pd.read_csv(data_path)

    # Mache Vorhersagen auf den neuen Daten
    predictions = predictor.predict(new_data)
    print(predictions)