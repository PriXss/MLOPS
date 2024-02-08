#in arbeit
'''import mlflow
import os

class MLFlowTracker:
    def __init__(self):
        # Setzt das mlflow-Tracking-Verzeichnis für das gesamte Projekt
        self.project_tracking_dir = os.path.abspath("../mlflow_tracking")

        mlflow.set_tracking_uri(f"file://{self.project_tracking_dir}")

    def mlflow_tracking(self, model_dir, model_name, ludwig_config_path, training_stats):
        # Model Logging mit MLflow
        with mlflow.start_run():
            mlflow.log_artifacts(model_dir, artifact_path=model_name)
            mlflow.log_params({"ludwig_config_path": ludwig_config_path})

            # Protokollieren der Metriken für Trainings-, Test- und Validierungsdaten
            for dataset_type, metrics in training_stats.items():
                for metric_name, metric_values in metrics.items():
                    for fold_index, fold_metric_values in enumerate(metric_values):
                        metric_key = f"{dataset_type}_{metric_name}_fold_{fold_index}"
                        # MLflow-Metrik protokollieren
                        mlflow.log_metric(metric_key, fold_metric_values)
'''