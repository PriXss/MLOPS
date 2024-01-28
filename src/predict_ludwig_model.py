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
        # Extrahiere die Vorhersagen aus dem Tupel
        predictions_df = predictions[0]
        return predictions_df

    def save_predictions(self, predictions, output_path):
        # Speichere Vorhersagen in einer CSV-Datei
        predictions.to_csv(output_path, index=False)


if __name__ == "__main__":
    # Pfade anpassen
    model_dir = "../models/Prognose_Kurs"
    data_path = "../data/transformierte_datei.csv"
    output_path = "../data/predicition.csv"

    # Lade den trainierten Modell
    predictor = ModelPredictor(model_dir)
    predictor.load_model()

    # Lade neue Daten
    new_data = pd.read_csv(data_path)

    # Mache Vorhersagen auf den neuen Daten
    predictions = predictor.predict(new_data)

    # Speichere Vorhersagen in einer CSV-Datei
    predictor.save_predictions(predictions, output_path)
