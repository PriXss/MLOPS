import os
import subprocess
import pandas as pd
import boto3
from dagster import asset
from ludwig.api import LudwigModel
import shutil
import tempfile
import zipfile
from datetime import datetime
import yaml
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
from botocore.exceptions import NoCredentialsError
import requests
import json
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, DataQualityPreset, RegressionPreset
from evidently.metrics import *
from evidently.tests import *
import warnings

session = boto3.session.Session()
s3_client = session.client(
    service_name= os.getenv("SERVICE_NAME"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=os.getenv("ENDPOINT_URL"),
    )


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
            api_experiment_run_src = os.path.join(os.getcwd(), '../my-dagster-project/results', 'api_experiment_run')
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
            shutil.rmtree(os.path.join(os.getcwd(), '../my-dagster-project/results/api_experiment_run'))


@asset(deps=[], group_name="TrainingPhase", compute_kind="LudwigModel")
def trainLudwigModelRegression(context) -> None:
    context.log.info('Trainin Running')
      # Pfade anpassen
    ludwig_config_file_name = os.getenv("TRAINING_CONFIG")  # Name der Ludwig-Konfigurationsdatei
    model_bucket_url = os.getenv("MODEL_BUCKET") 
    stock_name= os.getenv("TRAINING_STOCK_NAME")
    data_file = "data_"+stock_name+".csv"

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = MLFlowTrainer(model_bucket_url, ludwig_config_file_name=ludwig_config_file_name,
                            data_file_name=data_file)
    trainer.train_model()


@asset(group_name="DataCollectionPhase", compute_kind="DVCDataVersioning")
def fetchStockDataFromSource(context) -> None:
    context.log.info('Could Retrieve the Data from the API and store it in S3 accordingly')
   
    #Definieren von API Key und Symbol
    api_key = 'CEKRMJCF4Q07KVAC'
    symbol = os.getenv("STOCK_INPUT")
    

    # Verknüpfung von Symbolen und Firmennamen
    if symbol == 'AAPL':
        company_name = 'Apple'
    elif symbol == 'IBM':
        company_name = 'IBM'
    elif symbol == 'TSLA':
        company_name = 'Tesla'
    elif symbol == 'NKE':
        company_name = 'Nike'
    elif symbol == 'AMZN':
        company_name = 'Amazon'
    elif symbol == 'MSFT':
        company_name = 'Microsoft'
    elif symbol == 'GOOGL':
        company_name = 'Google'
    else:
        company_name = 'Company'  # Standardoption für Symbole


    #TimeSeries definieren im Pandas Format
    ts =TimeSeries(key=api_key, output_format='pandas')
    ti = TechIndicators(key=api_key, output_format='pandas')


    #URL anwählen
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'

    #Daten
    data, meta_data = ts.get_daily(symbol = symbol, outputsize='full')

    # Abrufen des RSI
    rsi_data, rsi_meta_data = ti.get_rsi(symbol=symbol, interval='daily', time_period=14, series_type='close')

    # Abrufen des EMA
    ema_data, ema_meta_data = ti.get_ema(symbol=symbol, interval='daily', time_period=10, series_type='close')

    # Sicherstellen, dass der Index bei den hinzugefügten Werten als Datum formatiert ist
    data.index = pd.to_datetime(data.index)
    rsi_data.index = pd.to_datetime(rsi_data.index)
    ema_data.index = pd.to_datetime(ema_data.index)

    #DataFrame
    df = pd.DataFrame(data)

    # Sortieren des DataFrame nach dem Index (Datum) in aufsteigender Reihenfolge
    df_sorted = df.sort_values(by='date', ascending=True)

    # Zusammenführen der Daten bzw. Hinzuügen der RSI und EMA Daten
    merged_data = pd.merge(data, rsi_data, how='left', left_index=True, right_index=True)
    merged_data = pd.merge(merged_data, ema_data, how='left', left_index=True, right_index=True)

    # Index-Spalte für Datum in eine normale Spalte umwandeln
    merged_data.reset_index(inplace=True)
    merged_data.rename(columns={'index': 'date'}, inplace=True)

    # Sortieren des zusammengeführten DataFrame nach dem Datum in aufsteigender Reihenfolge
    merged_data_sorted = merged_data.sort_values(by='date', ascending=True)

    # Spalte mit dem Symbol hinzufügen, Anmerkunge: Spalte Symbol zeigt Kürzel der Aktie, evtl. nicht nötig für Output
    merged_data['Symbol'] = symbol #falls gewünscht, kann die Spalte Symbol hinzugefügt werden

    #Index-Spalte für Datum in eine normale Spalte umwandeln, um den Qualitätscheck zu erleichtern
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date'}, inplace=True)


    #Quality Checks
    # Überprüfen, ob die Daten fehlende Werte enthalten
    if df.isnull().values.any():
        print("Warnung: Die Daten enthalten fehlende Werte. Überprüfen Sie die Datenqualität.")

    # Überprüfen, ob die Datumsangaben im richtigen Format vorliegen
    if not df['date'].apply(lambda x: isinstance(x, pd.Timestamp)).all():
        print("Warnung: Das Datum ist nicht im erwarteten Format.")


    print(data)       #Ausgabe der Standarddaten

    print(merged_data_sorted.head())     #Ausgabe der um RSI und EMA erweiterten Daten


    # Sortierten DataFrame als CSV exportieren
    csv_filename = f'data_{company_name}.csv'
    df_sorted.to_csv(csv_filename, index=True)

    # Erfolgsmeldung ausgeben
    print(f'Daten wurden als {csv_filename} exportiert.')

    # Aktuelles Arbeitsverzeichnis ausgeben
    print("Aktuelles Arbeitsverzeichnis:", os.getcwd())


    # Speicherung der CSV Datei
    output_directory = 'output'  # Der lokale Ordner 'output', der zum Speichern der CSV-Datei verwendet wird
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    csv_filepath = os.path.join(output_directory, csv_filename)

    # Speichern des zusammengeführten und sortierten DataFrames in einer CSV-Datei
    merged_data_sorted.to_csv(csv_filepath, index=False)

    # Minio S3-Konfiguration
    minio_bucket = os.getenv("STOCK_INPUT_BUCKET")          # S3 Bucket
    minio_object_name = csv_filename  # Name, den die Datei im Bucket haben soll

    # CSV-Datei auf Minio S3 hochladen
    try:
        s3_client.upload_file(csv_filepath, minio_bucket, minio_object_name)
        print(f'Datei wurde auf Minio S3 in den Bucket {minio_bucket} hochgeladen.')
    except FileNotFoundError:
        print(f'Die Datei {csv_filepath} wurde nicht gefunden.')
    except NoCredentialsError:
        print('Zugriffsberechtigungsfehler. Stellen Sie sicher, dass Ihre Minio S3-Zugriffsdaten korrekt sind.')
    except Exception as e:
        print(f'Ein Fehler ist aufgetreten: {str(e)}')



@asset(deps=[fetchStockDataFromSource] ,group_name="DataCollectionPhase", compute_kind="S3DataCollection")
def getStockData(context) -> None:
    
    bucket = os.getenv("STOCK_INPUT_BUCKET")
    stock_name= os.getenv("STOCK_NAME")
    file_name = "data_"+stock_name+".csv"
    obj = s3_client.get_object(Bucket= bucket, Key= file_name) 
    initial_df = pd.read_csv(obj['Body'])
    context.log.info('Data Extraction complete')
    context.log.info(initial_df.head())
    os.makedirs("data", exist_ok=True)
    initial_df.to_csv(f'data/{file_name}', index=False)        



@asset(deps=[getStockData], group_name="VersioningPhase", compute_kind="DVCDataVersioning")
def versionStockData(context) -> None:
    
    stock_name= os.getenv("STOCK_NAME")
    file_name = "data_"+stock_name+".csv"
    
    subprocess.run(["dvc", "add", f"data/{file_name}"])
    subprocess.run(["git", "add", f"data/{file_name}.dvc"])
    subprocess.run(["git", "add", "data/.gitignore"])
    subprocess.run(["git", "commit", "-m", "Add new Data for Prod Runs"])
    subprocess.run(["dvc", "push"])
    context.log.info('Data successfully versioned')



@asset(deps=[getStockData], group_name="ModelPhase", compute_kind="ModelAPI")
def requestToModel(context) -> None:
    
    
    bucket = os.getenv("STOCK_INPUT_BUCKET")
    stock_name = os.getenv("STOCK_NAME")
    file_name = "data_"+stock_name+".csv"
    obj = s3_client.get_object(Bucket= bucket, Key= file_name) 
    initial_df = pd.read_csv(obj['Body'])
    context.log.info('Data Extraction complete')
    context.log.info(initial_df.head())
    os.makedirs("prepareModelRequest", exist_ok=True)
    initial_df.to_csv(f'prepareModelRequest/{file_name}', index=False)  
    
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    payload = {
        "Datum": "2024-02-16",
        "Tageshoch": "143.19",
        "Datum": "18.02.2024",
        "Eroeffnung": "142.99",
        "RSI": "42.6",
        "EMA": "144.5158",
        "Umsatz":"31468926.0",
        "Tagestief": "140.14",
        }

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    model_name= os.getenv("MODEL_NAME")
    result_file_name = timestamp+"_Results_"+stock_name+"_"+model_name+".json"
    sessionRequest = requests.Session()
    requestUrl= os.getenv("MODEL_REQUEST_URL")
    response= sessionRequest.post(requestUrl,headers=headers,data=payload)
    context.log.info(f"Response: {response.json()}")
    os.makedirs("predictions", exist_ok=True)
    resultPayload = (payload, response.json())
    with open(f'predictions/{result_file_name}', 'w') as f:
        json.dump(resultPayload, f)
    
    path = f"predictions/{result_file_name}"
    bucket = os.getenv("PREDICTIONS_BUCKET")
    
    s3_client.upload_file(path, bucket, result_file_name)
    context.log.info("Upload to S3 succesful")
    subprocess.run(["dvc", "add", path])
    subprocess.run(["git", "add", path])
    subprocess.run(["git", "add", "predictions/.gitignore"])

    
    
@asset(deps=[requestToModel], group_name="VersioningPhase", compute_kind="DVCDataVersioning")
def versionPrediction(context) -> None:
  
    subprocess.run(["git", "commit", "-m", "Add new Predicition from Prod Run"])
    subprocess.run(["dvc", "push"])
    context.log.info('Prediction successfully versioned')


@asset(deps=[versionPrediction, versionStockData], group_name="MonitoringPhase", compute_kind="Reporting")
def monitoringAndReporting(context) -> None:
    
    ##### Ignore warnings #####
    warnings.filterwarnings('ignore')
    warnings.simplefilter('ignore')

    ##### Set file/bucket vars #####
    data_bucket_url = os.getenv("LOGS_BUCKET")
    data_stock = os.getenv("MONITORING_STOCK")
    data_model_version = os.getenv("MODEL_NAME")
    data_date = "20022024" #warsch automatisiert abgefragt werden?


    ##### Load data from the bucket #####
    obj= s3_client.get_object(Bucket=data_bucket_url, Key= data_date+"_"+data_stock+"_"+data_model_version+".csv" )
    df = pd.read_csv(obj['Body'])

    ##### Create report #####
    reference = df.iloc[50:,:]
    current = df.iloc[:50,:]

    report = Report(metrics=[
        DataDriftPreset(), 
        TargetDriftPreset(),
        RegressionPreset()
    ])

    report.run(reference_data=reference, current_data=current)
    
    reportName=os.getenv("REPORT_NAME")
    report.save_html(reportName)

    reportsBucket= os.getenv("REPORT_BUCKET")

#Das scheint noch nicht zu funktionieren?
#    result_obj=s3_client.Object(reportsBucket, "/"+data_stock+"/"+data_model_version+"/"+reportName)
#    result_obj.put(Body=open(reportName, 'rb'))
    context.log.info('Monitoring part running')
