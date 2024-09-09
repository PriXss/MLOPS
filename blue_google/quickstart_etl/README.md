# Starten der Pipelines
Zum Starten der Pipelines ohne vorherige Durchläufe (Beispielsweise nach einem crash und daraus resultierendem Neustart des Systems) müssen zunächst folgende Services deployed werden:

|        Service        |        Docker compose file        | 
|-----------------------|-----------------------------------|
| Minio Server          | docker-compose.yml                |   
| Dagster Dagit, Dagster Daemon & Postgres DB   | my-dagster-project\docker-compose.yml   | 
| MLFlow tracking Server   | mlflow-tracking-server\docker-compose.yml   |

Da innerhalb der Pipelines Git Operationen mittels dem in der Pipeline definierten GitHub user ausgeführt werden, muss beim build des Dagster Dagit/Daemon die environment Variable 
> TOKEN = *\*GitHub Token hier einsetzen\**

in Portainer gesetzt werden.

Sobald alle Services deployed sind, können die Pipelines wie folgt gestartet werden (Reihenfolge beachten):

1. Den Pipeline step **setupDVCandVersioningBucket** ausführen, um DVC zu initialisieren und die benötigten S3 Buckets anzulegen.
2. Den Pipeline step **fetchStockDataFromSource** ausführen, um einen ersten Rohdatensatz zu erzeugen.
3. Den Pipeline step **trainLudwigModelRegression** ausführen, um das Model zu trainieren.
4. Den Rest der Prediction Pipeline (**ModelPhase** und **MonitoringPhase**) ausführen, um eine Prediction und einen Report zu erhalten (Reports werden erst erstellt wenn zwei oder mehr Predictions vorhanden sind).