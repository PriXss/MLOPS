import os
import subprocess
import pandas as pd
import boto3
import botocore
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
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, DataQualityPreset, RegressionPreset
from evidently.metrics import *
from evidently.tests import *
import warnings
import mlflow
from botocore.exceptions import NoCredentialsError

timestamp=""


def pruefe_extreme_werte(reihe, grenzwerte):
        for spalte, (min_wert, max_wert) in grenzwerte.items():
            if reihe[spalte] < min_wert or reihe[spalte] > max_wert:
                return False  # Wert liegt außerhalb der Grenzen
        return True  # Alle Werte liegen innerhalb der Grenzen

def process_and_upload_symbol_data(
        symbol,
        api_key=os.getenv("API_KEY"),
        minio_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        minio_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        minio_endpoint=os.getenv("ENDPOINT_URL"),
        minio_bucket=os.getenv("STOCK_INPUT_BUCKET"),
        output_directory=os.getenv("OUTPUT_DIRECTORY")
        ):

        # S3-Verbindung herstellen
        s3 = boto3.client('s3', aws_access_key_id=minio_access_key, aws_secret_access_key=minio_secret_key, endpoint_url=minio_endpoint)

    # Speicherung der CSV Datei
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        #Definieren der Alpha Vantage Objekte im Pandas Format
        ts = TimeSeries(key=api_key, output_format='pandas')
        ti = TechIndicators(key=api_key, output_format='pandas')

        company_names = {
                'AAPL': 'Apple', 'IBM': 'IBM', 'TSLA': 'Tesla', 'NKE': 'Nike',
                'AMZN': 'Amazon', 'MSFT': 'Microsoft', 'GOOGL': 'Google'}

        company_name = company_names.get(symbol, 'Company')
        csv_filename = f'data_{company_name}.csv'
        minio_object_name = csv_filename
        csv_filepath = os.path.join(output_directory, csv_filename)

        # Daten abrufen
        data, _ = ts.get_daily(symbol=symbol, outputsize='full')
        rsi_data, _ = ti.get_rsi(symbol=symbol, interval='daily', time_period=14, series_type='close')
        ema_data, _ = ti.get_ema(symbol=symbol, interval='daily', time_period=10, series_type='close')
        sma_data, _ = ti.get_sma(symbol=symbol, interval='daily', time_period=10, series_type='close')
        dema_data, _ = ti.get_dema(symbol=symbol, interval='daily', time_period=10, series_type='close')

        # Sicherstellen, dass der Index bei den hinzugefügten Werten als Datum formatiert ist
        data.index = pd.to_datetime(data.index)
        rsi_data.index = pd.to_datetime(rsi_data.index)
        ema_data.index = pd.to_datetime(ema_data.index)
        sma_data.index = pd.to_datetime(sma_data.index)
        dema_data.index = pd.to_datetime(dema_data.index)

        # Zusammenführen der Daten mit Suffixen
        merged_data = pd.merge(data, rsi_data, how='left', left_index=True, right_index=True, suffixes=('', '_rsi'))
        merged_data = pd.merge(merged_data, sma_data, how='left', left_index=True, right_index=True, suffixes=('', '_sma'))
        merged_data = pd.merge(merged_data, ema_data, how='left', left_index=True, right_index=True, suffixes=('', '_ema'))
        merged_data = pd.merge(merged_data, dema_data, how='left', left_index=True, right_index=True, suffixes=('', '_dema'))

        # Füllen eventueller Lücken mit Nullen
        merged_data.fillna(0, inplace=True)
        merged_data.reset_index(inplace=True)

        # Umbenennen der Spalten in deutsche Bezeichnungen
        merged_data.rename(columns={
            'date': 'Datum', '1. open': 'Eroeffnung', '4. close': 'Schluss',
            '2. high': 'Tageshoch', '3. low': 'Tagestief', '5. volume': 'Umsatz',
        }, inplace=True)

        # Sortieren des zusammengeführten DataFrame nach dem Datum in aufsteigender Reihenfolge
        merged_data_sorted = merged_data.sort_values(by='Datum', ascending=True)


        # Quality Checks vor dem Sortieren und Speichern


        # Überprüfen, ob die Datumsangaben im richtigen Format vorliegen
        if not pd.to_datetime(merged_data_sorted['Datum'], errors='coerce').notnull().all():
            print(f"Warnung: Einige Datumsangaben für {symbol} sind nicht im erwarteten Format.")
        
        # Sicherstellen, dass der Wert für das neueste Datum aktualisiert wird
        neuestes_datum = merged_data_sorted['Datum'].max()

        #Definition der Grenzwerte
        grenzwerte = {
            'Eroeffnung': (1, 5000),
            'Tageshoch': (1, 5000),
            'Tagestief': (1, 5000),
            'Schluss': (1, 5000),
        }
        # Prüfen auf extreme Werte für den neuesten Datensatz
        neuestes_datum = merged_data_sorted['Datum'].max()
        neueste_zeile = merged_data_sorted[merged_data_sorted['Datum'] == neuestes_datum]
        if not pruefe_extreme_werte(neueste_zeile.iloc[0], grenzwerte):
            print(f"Warnung: Extreme Werte für {symbol} gefunden. Upload der Datei wird abgelehnt.")
            upload_abgelehnt = True
        else:
            upload_abgelehnt = False  # Setzen Sie upload_abgelehnt nur dann auf False, wenn die Überprüfungen bestanden sind

        # Quality Check für fehlende Werte


        # Überprüfen der Werte für 'Eroeffnung', 'Tageshoch', 'Tagestief' und 'Schluss' am neuesten Datum
        neueste_zeile = merged_data_sorted[merged_data_sorted['Datum'] == neuestes_datum]

        # Überprüfen, ob einer der Werte 0 ist
        werte_zu_pruefen = ['Eroeffnung', 'Tageshoch', 'Tagestief', 'Schluss']
        # Eröffnunfs-, Tageshoch-, Tagestief- und Schlusswerte dürfen nicht 0 sein
        upload_abgelehnt = False

        for spalte in werte_zu_pruefen:
            if neueste_zeile[spalte].iloc[0] == 0:
                print(f"Der neueste Wert für '{spalte}' ist 0. Upload der Datei wird abgelehnt.")
                upload_abgelehnt = True
                break  # Beendet die Schleife, da mindestens ein Wert 0 ist

        # Sortierten DataFrame als CSV exportieren
        csv_filepath = os.path.join(output_directory, csv_filename)
        merged_data_sorted.to_csv(csv_filepath, index=False)


        if not upload_abgelehnt:
        # Wenn keiner der Werte 0 ist, wird CSV-Datei auf Minio S3 hochgeladen
            try:
                s3.upload_file(csv_filepath, minio_bucket, minio_object_name)
                print(f'Datei wurde auf Minio S3 in den Bucket {minio_bucket} hochgeladen.')
            except FileNotFoundError:
                print(f'Die Datei {csv_filepath} wurde nicht gefunden.')
            except NoCredentialsError:
                print('Zugriffsberechtigungsfehler. Stellen Sie sicher, dass Ihre Minio S3-Zugriffsdaten korrekt sind.')
            except Exception as e:
                print(f'Ein Fehler ist aufgetreten: {str(e)}')


session = boto3.session.Session()
s3_client = session.client(
    service_name= os.getenv("SERVICE_NAME"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=os.getenv("ENDPOINT_URL"),
    )

##---------------------training area----------------------------------------------
class MLFlowTrainer:
    def __init__(self, model_bucket_url, model_name="", ludwig_config_file_name="", data_file_name=""):
        self.model_bucket_url = model_bucket_url
        self.model_name = model_name
        self.ludwig_config_file_name = ludwig_config_file_name
        self.data_file_name = data_file_name

        # Setzen der Bucket-URLs
        self.mlflow_bucket_url = "mlflowtracking"
        self.data_bucket_url = "data"
        self.model_configs_bucket_url = "modelconfigs"
        self.access_key_id = "test"
        self.secret_access_key = "testpassword"
        self.endpoint_url = "http://85.215.53.91:9000"

        # Setzen der Umgebungsvariablen für den Zugriff auf Buckets
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint_url

        # Initialisierung des globalen Timestamps
        self.global_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    def train_model(self):
        # Extrahieren der Variable data_name aus der Textdatei im Bucket
        data_name = self.get_data_name_from_bucket()

        # Pfade anpassen
        data_file = f"data_{data_name}.csv"

        # Laden der Daten aus dem Bucket
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.data_bucket_url, Key=data_file)
        data = pd.read_csv(obj['Body'])

        # Starten des MLflow-Laufs
        with mlflow.start_run() as run:
            self.run_id = run.info.run_id  # Run-ID speichern

            try:
                # Temporäre Datei für die Ludwig-Konfigurationsdatei erstellen
                temp_ludwig_config_file = tempfile.NamedTemporaryFile(delete=False)
                try:
                    # Ludwig-Konfigurationsdatei aus dem Bucket herunterladen und lokal speichern
                    s3.download_fileobj(Bucket=self.model_configs_bucket_url, Key=self.ludwig_config_file_name,
                                        Fileobj=temp_ludwig_config_file)

                    temp_ludwig_config_file.close()

                    # Extrahiere den Modellnamen aus der Ludwig-Konfigurationsdatei
                    model_name = self.extract_model_name(temp_ludwig_config_file.name)

                    # Namensgebung ML Run
                    mlflow.set_tag('mlflow.runName', f'{data_name}_{model_name}_{self.global_timestamp}')

                    # Ludwig-Modell trainieren
                    ludwig_model = LudwigModel(config=temp_ludwig_config_file.name)
                    train_stats, _, _ = ludwig_model.train(dataset=data, split=[0.8, 0.1, 0.1],
                                                           skip_save_processed_input=True)

                    # Loggen der Parameter
                    self.log_params(data_name, data_file, model_name)

                    # Loggen der Metriken
                    self.log_metrics(train_stats)

                    # Speichern von Artefakten
                    with tempfile.TemporaryDirectory() as temp_dir_model:
                        # Modell speichern
                        model_path = os.path.join(temp_dir_model, model_name)
                        ludwig_model.save(model_path)
                        mlflow.log_artifact(model_path, artifact_path='')

                    # Speichern von Artefakten
                    self.save_model_to_s3(ludwig_model, model_name, data_name)

                    # Hochladen der meta.yaml-Datei in den S3-Bucket modelconfigs
                    local_path = os.path.join(os.getcwd(), 'mlruns', '0', self.run_id)
                    self.upload_meta_yaml_to_s3(local_path)

                finally:
                    os.unlink(temp_ludwig_config_file.name)

            finally:
                # MLflow-Lauf beenden
                mlflow.end_run()

                # Den Ordner des aktuellen MLflow-Laufs komprimieren und als Zip-Datei hochladen
                zip_file_name = f"{self.run_id}.zip"
                zip_file_path = os.path.join(os.getcwd(), 'mlruns', '0', zip_file_name)
                shutil.make_archive(os.path.join(os.getcwd(), 'mlruns', '0', self.run_id), 'zip', local_path)
                s3.upload_file(zip_file_path, "mlflowtracking", zip_file_name)

                # Lokale Runs nach dem Upload löschen
                shutil.rmtree(os.path.join(os.getcwd(),'mlruns'))

    def get_data_name_from_bucket(self):
        # Verbindung zum S3-Client herstellen
        s3 = boto3.client('s3')

        # Datei mit der Variable data_name aus dem Bucket modelconfigs herunterladen
        obj = s3.get_object(Bucket=self.model_configs_bucket_url, Key="data_name.txt")
        data_name = obj['Body'].read().decode('utf-8').strip()

        return data_name

    def upload_directory_to_s3(self, local_path, bucket, s3_path):
        s3_client = boto3.client('s3')
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_path)
                s3_file = os.path.join(s3_path, relative_path)
                s3_client.upload_file(local_file, bucket, s3_file)

    def upload_meta_yaml_to_s3(self, local_path):
        meta_yaml_path = os.path.join(local_path, "..", "meta.yaml")
        if os.path.exists(meta_yaml_path):
            # Lese den Inhalt der meta.yaml-Datei
            with open(meta_yaml_path, 'r') as file:
                meta_yaml_content = yaml.safe_load(file)

            # Passe den artifact_location-Pfad relativ zum aktuellen Verzeichnis an
            meta_yaml_content['artifact_location'] = './mlruns/0'

            # Speichere den angepassten Inhalt zurück in die meta.yaml-Datei
            with open(meta_yaml_path, 'w') as file:
                yaml.dump(meta_yaml_content, file)

            # Lade die angepasste meta.yaml-Datei in den S3-Bucket hoch
            s3_client = boto3.client('s3')
            s3_client.upload_file(meta_yaml_path, self.model_configs_bucket_url, "meta.yaml")

    def extract_model_name(self, yaml_file_path):
        with open(yaml_file_path, 'r') as file:
            yaml_content = yaml.safe_load(file)
            model_name = None
            if 'model' in yaml_content and 'type' in yaml_content['model']:
                model_name = yaml_content['model']['type']
            return model_name

    def save_model_to_s3(self, model, model_name, data_name):
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
            model_zip_file_name = f"trained_model_{data_name}_{model_name}_{self.global_timestamp}.zip"
            api_zip_file_name = f"api_experiment_run_{data_name}_{model_name}_{self.global_timestamp}.zip"

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

        shutil.rmtree(os.path.join(os.getcwd(), 'results'))

    def log_params(self, data_name, data_file, model_name):
        mlflow.log_param("Stock", data_name)
        mlflow.log_param("ludwig_config_file_name", self.ludwig_config_file_name)
        mlflow.log_param("data_file_name", data_file)
        mlflow.log_param("Model", model_name)

    def log_metrics(self, train_stats):
        phases = ['test', 'training', 'validation']
        for phase in phases:
            section = train_stats[phase]["Schluss"]
            for metric_name, values in section.items():
                for idx, value in enumerate(values):
                    mlflow.log_metric(f"{phase}_{metric_name}", value)


@asset(deps=[], group_name="TrainingPhase", compute_kind="LudwigModel")
def trainLudwigModelRegression(context) -> None:
    context.log.info('Trainin Running')
      # Pfade anpassen
    ludwig_config_file_name = os.getenv("TRAINING_CONFIG")  # Name der Ludwig-Konfigurationsdatei
    model_bucket_url = os.getenv("MODEL_BUCKET") 
    stock_name= os.getenv("STOCK_NAME")
    data_file = "data_"+stock_name+".csv"

    # Instanz der Klasse erstellen und das Modell trainieren
    trainer = MLFlowTrainer(model_bucket_url, ludwig_config_file_name=ludwig_config_file_name,
                            data_file_name=data_file)
    trainer.train_model()


##-----------------training area ----------------------------------------------------




@asset(deps=[], group_name="DVCVersioning", compute_kind="DVC")
def setupDVCandVersioningBucket(context) -> None:
    context.log.info('Settings for DVC and S3')
    

    
    # setup default remote
    timestampTemp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    timestamp=timestampTemp

    subprocess.run(["dvc", "remote", "modify", "versioning", "url" "s3://"+ os.getenv("VERSIONING_BUCKET")+"/"+timestamp])
    subprocess.run(["dvc" "remote" "modify" "versioning" "endpointurl" "http://http://85.215.53.91:9000"])
    subprocess.run(["dvc" "remote" "modify" "versioning" "endpointurl" "http://http://85.215.53.91:9000"])
    subprocess.run(["dvc" "remote" "modify" "versioning" "endpointurl" "http://http://85.215.53.91:9000"])

    subprocess.run(["dvc", "push"])
    subprocess.run(["git", "add", "."])
    subprocess.run(["git", "commit", "-m", "Add new DVC Config for todays run"])
    subprocess.run(["git", "push" "origin" "HEAD:origin/DagsterPipelineProdRun" "--force"])
    


  
@asset(deps=[setupDVCandVersioningBucket], group_name="DataCollectionPhase", compute_kind="DVCDataVersioning")
def fetchStockDataFromSource(context) -> None:
    

    symbols = ['AAPL', 'IBM', 'TSLA', 'NKE', 'AMZN', 'MSFT', 'GOOGL']

    print("Starte den Prozess...")
        
    processed_symbols = []  # Liste, um verarbeitete Symbole zu verfolgen

    for symbol in symbols:
        if symbol not in processed_symbols:  # Überprüfen, ob das Symbol bereits verarbeitet wurde
            print(f"Verarbeite Symbol: {symbol}")
            process_and_upload_symbol_data(
                    api_key='69SMJJ4C2JIW86LI',
                    minio_access_key='test',
                    minio_secret_key='testpassword',
                    minio_endpoint='http://85.215.53.91:9000',
                    minio_bucket='data',
                    symbol=symbol,
                    output_directory='output'
                )
            processed_symbols.append(symbol)  # Füge das Symbol zur Liste der verarbeiteten Symbole hinzu
        else:
            print(f"Das Symbol {symbol} wurde bereits verarbeitet, überspringe...")

    print("Prozess abgeschlossen.")


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

    subprocess.run(["dvc", "add", f"data/{file_name}"])
    context.log.info(subprocess.run(["dvc", "status"]))

    subprocess.run(["dvc", "push"])
    subprocess.run(["git", "add", f"data/{file_name}.dvc"])
    subprocess.run(["git", "commit", "-m", "Add new Data for Todays run"])
    subprocess.run(["git", "push"])
    context.log.info('Data successfully versioned')



@asset(deps=[getStockData], group_name="ModelPhase", compute_kind="ModelAPI")
def requestToModel(context) -> None:
     
    ##### Get Input Data from csv file in S3 bucket ##### 
    bucket = os.getenv("STOCK_INPUT_BUCKET")
    stock_name = os.getenv("STOCK_NAME")
    file_name = "data_"+stock_name+".csv"
    obj = s3_client.get_object(Bucket= bucket, Key= file_name) 
    df = pd.read_csv(obj['Body'])
    context.log.info('Data Extraction complete')
    context.log.info(df.head())
    
    ##### Prepare the payload and headers #####  
    df = df.tail(1) # using only the last (most recent) row of Input data
    payload = df.to_dict('list') # The API currently only accepts Dictinoary (String) Parameters
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    ##### API call #####
    model_name= os.getenv("MODEL_NAME")
    result_file_name = "Predictions_"+stock_name+"_"+model_name+".csv"
    sessionRequest = requests.Session()
    requestUrl= os.getenv("MODEL_REQUEST_URL")
    response= sessionRequest.post(requestUrl,headers=headers,data=payload)
    context.log.info(f"Response: {response.json()}")
    os.makedirs("predictions", exist_ok=True)
    resultJson = {**payload, **response.json()}
    df_result = pd.DataFrame(resultJson, index=[0])

    ##### Upload prediction to S3 bucket #####
    path = f"predictions/{result_file_name}"
    bucket = os.getenv("PREDICTIONS_BUCKET")
    try:
        s3_client.head_object(Bucket=bucket, Key=result_file_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # Prediction-file for stock/model combination doesn't yet exist in the s3 bucket
            df_result.to_csv(path, mode='w', index=False, header=True) # Create prediction-file (incl. header)
        else:
            # Other error
            context.log.info('Connection to S3-bucket failed!')
    else:
        # Prediction-file exists
        s3_client.download_file(bucket, result_file_name, path) # Download prediction-file to disk
        df_result.to_csv(path, mode='a', index=False, header=False) # Append prediction file (excl. header)
        
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


@asset(deps=[versionPrediction], group_name="MonitoringPhase", compute_kind="Reporting")
def monitoringAndReporting(context) -> None:
    
    ##### Ignore warnings #####
    warnings.filterwarnings('ignore')
    warnings.simplefilter('ignore')

    ##### Set file/bucket vars #####
    data_bucket_url = os.getenv("PREDICTIONS_BUCKET")
    data_stock = os.getenv("STOCK_NAME")
    data_model_version = os.getenv("MODEL_NAME")

    ##### Load data from the bucket #####
    obj = s3_client.get_object(Bucket=data_bucket_url, Key= "Predictions_"+data_stock+"_"+data_model_version+".csv" )
    df = pd.read_csv(obj['Body'])
    
    ##### Data prep #####
    df = df.rename(columns={'Schluss': 'target', 'Schluss_predictions': 'prediction'}) # Rename columns to fit evidently input
    df['prediction'] = df['prediction'].shift(1) # Shift predictions to match them with the actual target (close price of the following day)
    df = df.iloc[1:] # drop first row, as theres no matching prediction 
    
    ##### Create report #####
    #Reference-Current split
    #reference = df.iloc[int(len(df.index)/2):,:]
    #current = df.iloc[:int(len(df.index)/2),:]

    report = Report(metrics=[
        #DataDriftPreset(), 
        #TargetDriftPreset(),
        DataQualityPreset(),
        RegressionPreset()
    ])

    reportName=os.getenv("REPORT_NAME")
    report.run(reference_data=None, current_data=df)
    report.save_html(reportName)    

    reportsBucket= os.getenv("REPORT_BUCKET")
    path = data_stock+"/"+data_model_version+"/"+reportName

    s3_client.upload_file(reportName ,reportsBucket, path)      
    