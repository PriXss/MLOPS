import os
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
import subprocess
import pandas as pd
import boto3
import botocore
from dagster import asset, job
from ludwig.api import LudwigModel
import shutil
import tempfile
import zipfile
from datetime import datetime, date
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
from botocore.exceptions import NoCredentialsError, ClientError
from dvc.repo import Repo
import sys
import time

timestamp=""
timestampTraining=""
model = os.getenv("MODEL_NAME")
data = os.getenv("STOCK_NAME")
timestamp_string= str(timestamp)
timestampTraining_string = str(timestampTraining)


session = boto3.session.Session()
s3_client = session.client(
    service_name= os.getenv("SERVICE_NAME"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=os.getenv("ENDPOINT_URL"),
    )


def pruefe_extreme_werte(reihe, grenzwerte):
        for spalte, (min_wert, max_wert) in grenzwerte.items():
            if reihe[spalte] < min_wert or reihe[spalte] > max_wert:
                return False  # Wert liegt außerhalb der Grenzen
        return True  # Alle Werte liegen innerhalb der Grenzen

def process_and_upload_symbol_data(
        symbol,
        api_key=os.getenv("API_KEY"),
        output_directory=os.getenv("OUTPUT_DIRECTORY"),
        minio_bucket=os.getenv("OUTPUT_DIRECTORY")
        ):
    
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
                s3_client.upload_file(csv_filepath ,minio_bucket, minio_object_name)  
                print(f'Datei wurde auf Minio S3 in den Bucket {minio_bucket} hochgeladen.')
                subprocess.run(["dvc", "add", f"{output_directory}/{csv_filename}"])
                print('DVC add successfully')
                subprocess.run(["dvc", "commit"])
                subprocess.run(["dvc", "push"])
                print('DVC push successfully')  
            except FileNotFoundError:
                print(f'Die Datei {csv_filepath} wurde nicht gefunden.')
            except NoCredentialsError:
                print('Zugriffsberechtigungsfehler. Stellen Sie sicher, dass Ihre Minio S3-Zugriffsdaten korrekt sind.')
            except Exception as e:
                print(f'Ein Fehler ist aufgetreten: {str(e)}')
        subprocess.run(["git", "add", f"{output_directory}/{csv_filename}.dvc"]) 
        subprocess.run(["git", "add", f"{output_directory}/.gitignore"]) 

def check_and_create_bucket(bucket_name):
    try:
        # Attempt to head the bucket
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"The bucket '{bucket_name}' already exists.")
    except ClientError as e:
        # If a client error is thrown, then the bucket does not exist
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket does not exist, create it
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"The bucket '{bucket_name}' has been created successfully.")
            except ClientError as create_error:
                print(f"Failed to create bucket '{bucket_name}': {create_error}")
        else:
            # Handle other exceptions if needed
            print(f"Failed to check bucket '{bucket_name}': {e}")

def install(package):
    subprocess.run([sys.executable, "-m", "pip", "install", package], check=True)

def create_and_write_serve_model_script():
    content = """cd ${MODEL_NAME}
    ludwig serve"""
    with open("serve_model.sh", "w") as file:
        file.write(content)

def create_and_write_serve_model_pyscript():
    content = """import os

directory = os.environ.get('model_name')
print(f'changing directory to {directory}')
os.system(f'cd {directory}')
os.system(f'ludwig serve -m {directory}')
"""
    with open("serve_model.py", "w") as file:
        file.write(content)

def create_and_write_dockerfile():
    docker_file_content = """FROM ludwigai/ludwig
    ARG model_name
    ENV model_name $model_name
    WORKDIR /src
    COPY ./ /src
    EXPOSE 8000
    RUN chmod +x serve_model.sh
    RUN chmod +x docker
    ENTRYPOINT ["python3", "serve_model.py"]
    """
    with open("Dockerfile", "w") as file:
        file.write(docker_file_content)

##---------------------training area----------------------------------------------
@asset(group_name="DVCVersioning", compute_kind="DVC")
def setupDVCandVersioningBucketForTraining(context) -> None:
    context.log.info('Settings for DVC and S3 for Training')
    
    # setup default remote
    timestampTemp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    timestampTraining=timestampTemp

    s3_client.put_object(
    Bucket= os.getenv("VERSIONING_TRAINING_BUCKET"),
    Key= timestampTraining+"/"
    )
    
    subprocess.run(["git", "config", "--global", "user.name", "Marcel Thomas"])
    subprocess.run(["git", "config", "--global", "user.email", "73349327+PriXss@users.noreply.github.com"])
    
    subprocess.run(["git", "pull"])
    print("repo is up to date")
        
    subprocess.run(["dvc", "remote", "modify", "versioning", "url", "s3://"+ os.getenv("VERSIONING_TRAINING_BUCKET") + "/" +timestampTraining])
    subprocess.run(["dvc", "commit"])
    subprocess.run(["dvc", "push"])

    subprocess.run(["git", "add", "../.dvc/config"])


class MLFlowTrainer:
    def __init__(self, model_name="", ludwig_config_file_name="", data_name="", ludwig_config_path="",
                 model_bucket_url="", mlflow_bucket_url="", data_bucket_url="", model_configs_bucket_url=""):
        self.run_id = None
        self.model_name = model_name
        self.ludwig_config_file_name = ludwig_config_file_name
        self.data_file_name = data_name
        self.path_to_ludwig_config_file = ludwig_config_path

        # Setzen der Bucket-URLs
        self.model_bucket_url = model_bucket_url
        self.mlflow_bucket_url = mlflow_bucket_url
        self.data_bucket_url = data_bucket_url
        self.model_configs_bucket_url = model_configs_bucket_url

        # Setzen der Zugriffsparameter für S3 via Umgebungsvariablen
        self.access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.endpoint_url = os.getenv("ENDPOINT_URL")

        # Setzen der Umgebungsvariablen für den Zugriff auf Buckets
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint_url

        # Initialisierung des globalen Timestamps
        self.global_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    def train_model(self):

        # Zuweisen der gewünschten Daten
        data_file = f"data_{self.data_file_name}.csv"

        # Laden der Daten aus dem S3-Bucket
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.data_bucket_url, Key=data_file)
        data = pd.read_csv(obj['Body'])

        # Starten des MLflow-Laufs
        with mlflow.start_run() as run:
            self.run_id = run.info.run_id  # Run-ID speichern

            try:
                # Konfigurationsdatei laden
                config_file_path = os.path.join(self.path_to_ludwig_config_file, self.ludwig_config_file_name)

                # Kurzer Check ob die Datei existiert
                if os.path.isfile(config_file_path):
                    # Dateiinhalt anzeigen (optional für Debugging)
                    with open(config_file_path, 'r') as file:
                        ludwig_file_content = file.read()
                        print("Dateiinhalt:", ludwig_file_content)

                    # Konfigurationsdatei versionieren
                    temp_path = os.path.join(os.getcwd(), 'config_versioned', self.ludwig_config_file_name)
                    shutil.copyfile(config_file_path, temp_path)
                    self.version_control(temp_path)

                    # Ludwig-Modell trainieren
                    ludwig_model = LudwigModel(config=config_file_path)
                    train_stats, _, _ = ludwig_model.train(dataset=data, skip_save_processed_input=True)

                    # Speichern und Hochladen des trainierten Modells
                    self.save_model_to_s3(ludwig_model, self.model_name, self.data_file_name)

                else:
                    print("Die Datei existiert nicht:", config_file_path)

            finally:
                # Den MLflow-Lauf beenden und das Zip-Archiv in S3 hochladen
                self.upload_mlflow_run()

    def save_model_to_s3(self, model, model_name, data_name):
        s3 = boto3.client('s3')

        # Temporäres Verzeichnis für das Modell erstellen
        with tempfile.TemporaryDirectory() as temp_dir_model:
            model_path = os.path.join(temp_dir_model, 'model')
            model.save(model_path)

            # Modell-Verzeichnis komprimieren
            model_zip_file_name = f"trained_model_{data_name}_{model_name}_{self.global_timestamp}.zip"
            model_zip_file_path = os.path.join(temp_dir_model, model_zip_file_name)

            with zipfile.ZipFile(model_zip_file_path, 'w') as zipf:
                for root, dirs, files in os.walk(model_path):
                    for file in files:
                        zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), model_path))

            # Zip-Datei in den S3-Bucket hochladen
            s3.upload_file(model_zip_file_path, self.model_bucket_url, model_zip_file_name)

        # Versionieren der hochgeladenen Artefakte
        self.version_control(os.path.join(os.getcwd(), 'results'))

    def upload_mlflow_run(self):
        # Pfad zum MLflow Run-Ordner
        local_path = os.path.join(os.getcwd(), 'mlruns', '0', self.run_id)

        # Komprimieren des MLflow-Laufordners
        zip_file_name = f"{self.run_id}.zip"
        zip_file_path = os.path.join(os.getcwd(), 'mlruns', '0', zip_file_name)
        shutil.make_archive(os.path.join(os.getcwd(), 'mlruns', '0', self.run_id), 'zip', local_path)

        # Hochladen des Zip-Archivs in S3
        s3 = boto3.client('s3')
        s3.upload_file(zip_file_path, os.getenv("MLFLOW_BUCKET"), zip_file_name)

        # Versionieren des MLflow Run-Ordners
        self.version_control(os.path.join(os.getcwd(), 'mlruns'))

    def version_control(self, path):
        subprocess.run(["dvc", "add", path])
        subprocess.run(["dvc", "commit"])
        subprocess.run(["dvc", "push"])
        subprocess.call(["git", "add", f"{path}.dvc"])
        subprocess.call(["git", "add", ".gitignore"])


class AlpacaTrader:
    def __init__(self, api_key, api_secret, base_url, threshold, stocks, context, prediction_type='regression'):
        """
        Initialisiert den AlpacaTrader mit API-Schlüsseln, Basis-URL, Schwellenwert und Aktieninformationen.
        """
        self.api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')
        self.threshold = threshold
        self.stocks = stocks  # Dictionary mit Aktienkürzeln und deren Vorhersagen
        self.context = context
        self.logger = context.log
        self.prediction_type = prediction_type  # 'regression' oder 'classification'
        self.logger.info("AlpacaTrader initialized")

    def get_account_info(self):
        """
        Ruft Kontoinformationen vom Alpaca-API ab und gibt diese zurück.
        """
        account = self.api.get_account()
        info = {
            "cash": float(account.cash),
            "buying_power": float(account.buying_power),
            "equity": float(account.equity),
            "last_equity": float(account.last_equity)
        }
        self.logger.info(f"Account Info: {info}")
        return info

    def get_latest_close(self, ticker):
        """
        Ruft den letzten verfügbaren Schlusskurs der angegebenen Aktie ab.
        Falls der Schlusskurs für den aktuellen Tag nicht verfügbar ist, wird der Schlusskurs vom vorherigen Tag verwendet.
        """
        try:
            self.logger.info(f"Attempting to retrieve latest close for {ticker}")

            # Hole die letzten 5 Tage an Bars, um sicherzustellen, dass wir den letzten verfügbaren Schlusskurs bekommen
            bars = self.api.get_bars(ticker, TimeFrame.Day, limit=5)

            if not bars:
                self.logger.warning(f"No bars returned for {ticker}")
                return None

            # Suche den letzten verfügbaren Schlusskurs
            latest_close = None
            for bar in bars:
                if bar.c is not None:
                    latest_close = bar.c
                    break

            if latest_close is None:
                self.logger.warning(f"No close price found in the returned bars for {ticker}")
                return None

            self.logger.info(f"Latest close for {ticker}: {latest_close}")
            return latest_close

        except tradeapi.RestError as e:
            self.logger.error(f"API error retrieving latest close for {ticker}: {e}")
        except Exception as e:
            self.logger.error(f"Error retrieving latest close for {ticker}: {e}")
        return None

    def get_prediction(self, ticker):
        """
        Ruft die Vorhersage für die angegebene Aktie ab.
        Die Vorhersage hängt vom Modelltyp ab (Regression oder Klassifikation).
        """
        prediction = self.stocks.get(ticker)
        self.logger.info(f"Prediction for {ticker}: {prediction}")
        return prediction

    def place_buy_order(self, ticker, qty):
        """
        Platziert eine Kauforder für die angegebene Aktie.
        """
        self.logger.info(f"Placing buy order for {ticker}: {qty} shares")
        buy_order = self.api.submit_order(
            symbol=ticker,
            qty=qty,
            side='buy',
            type='market',
            time_in_force='gtc'
        )
        self.logger.info(f"Buy order placed for {ticker}: {buy_order}")
        return buy_order

    def place_sell_order(self, ticker, qty):
        """
        Platziert eine Verkaufsorder für die angegebene Aktie.
        """
        self.logger.info(f"Placing sell order for {ticker}: {qty} shares")
        sell_order = self.api.submit_order(
            symbol=ticker,
            qty=qty,
            side='sell',
            type='market',
            time_in_force='gtc'
        )
        self.logger.info(f"Sell order placed for {ticker}: {sell_order}")
        return sell_order

    def check_position(self, ticker):
        """
        Überprüft die Position der angegebenen Aktie im Portfolio.
        """
        positions = self.api.list_positions()
        for position in positions:
            if position.symbol == ticker:
                qty = float(position.qty)
                self.logger.info(f"Position for {ticker}: {qty} shares")
                return qty
        self.logger.info(f"No position found for {ticker}")
        return 0

    def calculate_potential_gains_and_losses(self):
        """
        Berechnet die potenziellen Gewinne und Verluste für die Aktien, wenn das Modell Regressionsvorhersagen liefert.
        """
        potential_gains = {}
        potential_losses = {}

        for ticker in self.stocks:
            latest_close = self.get_latest_close(ticker)
            predicted_close = self.get_prediction(ticker)

            if latest_close is None or predicted_close is None:
                self.logger.error(f"Error: Could not retrieve necessary price information for {ticker}.")
                continue

            price_difference = (predicted_close - latest_close) / latest_close
            self.logger.info(f"Price difference for {ticker}: {price_difference}")

            if price_difference > 0:
                potential_gains[ticker] = price_difference
            else:
                potential_losses[ticker] = price_difference

        self.logger.info(f"Potential gains: {potential_gains}")
        self.logger.info(f"Potential losses: {potential_losses}")

        return potential_gains, potential_losses

    def determine_best_and_worst(self, potential_gains, potential_losses):
        """
        Bestimmt die Aktie mit dem höchsten vorhergesagten Gewinn und die Aktie im Portfolio mit dem höchsten Verlust.
        """
        best_gain_ticker = max(potential_gains, key=potential_gains.get, default=None)
        worst_loss_ticker = min(potential_losses, key=potential_losses.get, default=None)

        self.logger.info(f"Best gain ticker: {best_gain_ticker}")
        self.logger.info(f"Worst loss ticker: {worst_loss_ticker}")

        return best_gain_ticker, worst_loss_ticker

    def sell_worst_loss_stock(self, worst_loss_ticker, positions):
        """
        Verkauft die Aktie im Portfolio mit dem höchsten Verlust.
        """
        if worst_loss_ticker and worst_loss_ticker in positions:
            qty = positions[worst_loss_ticker]
            sell_order = self.place_sell_order(worst_loss_ticker, qty)
            self.logger.info(f"Sell order executed for {worst_loss_ticker}: {sell_order}")
            return True
        return False

    def buy_best_gain_stock(self, best_gain_ticker, account_info):
        """
        Kauft die Aktie mit dem höchsten vorhergesagten Gewinn.
        """
        if best_gain_ticker:
            latest_close_best_gain = self.get_latest_close(best_gain_ticker)
            if latest_close_best_gain:
                max_shares = int(account_info['cash'] // latest_close_best_gain)
                self.logger.info(f"Maximum shares to buy for {best_gain_ticker}: {max_shares}")
                if max_shares > 0:
                    buy_order = self.place_buy_order(best_gain_ticker, max_shares)
                    self.logger.info(f"Buy order executed for {best_gain_ticker}: {buy_order}")
                else:
                    self.logger.warning(f"Not enough cash to buy shares of {best_gain_ticker}.")

    def execute_trade(self):
        """
        Führt die Handelslogik basierend auf Vorhersagen durch, die entweder durch Regression oder Klassifikation erstellt wurden.
        """
        self.logger.info("Executing trade")
        account_info = self.get_account_info()

        # Holen der aktuellen Positionen
        positions = {pos.symbol: float(pos.qty) for pos in self.api.list_positions()}
        self.logger.info(f"Current positions: {positions}")

        if self.prediction_type == 'regression':
            # Berechnung der potenziellen Gewinne und Verluste für Regression
            potential_gains, potential_losses = self.calculate_potential_gains_and_losses()
            best_gain_ticker, worst_loss_ticker = self.determine_best_and_worst(potential_gains, potential_losses)

            # Verkauf der Aktie mit dem höchsten Verlust
            if self.sell_worst_loss_stock(worst_loss_ticker, positions):
                time.sleep(3)
                account_info = self.get_account_info()
                # Kauf der Aktie mit dem höchsten Gewinn, wenn sie genug Cash haben
                self.buy_best_gain_stock(best_gain_ticker, account_info)

            else:
                # Wenn keine Aktien verkauft wurden und es eine beste Gewinnaktie gibt
                if best_gain_ticker and best_gain_ticker not in positions:
                    time.sleep(3)
                    account_info = self.get_account_info()
                    self.buy_best_gain_stock(best_gain_ticker, account_info)

        elif self.prediction_type == 'classification':
            self.logger.info("Processing classification predictions")
            for ticker in self.stocks:
                prediction = self.get_prediction(ticker)

                if prediction == 'buy':
                    # Wenn das Modell empfiehlt, die Aktie zu kaufen
                    if ticker not in positions:
                        # Wenn die Aktie nicht im Portfolio ist
                        latest_close = self.get_latest_close(ticker)
                        if latest_close:
                            max_shares = int(account_info['cash'] // latest_close)
                            self.logger.info(f"Maximum shares to buy for {ticker}: {max_shares}")
                            if max_shares > 0:
                                self.place_buy_order(ticker, max_shares)
                                time.sleep(3)
                                account_info = self.get_account_info()  # Aktualisierte Kontoinformationen nach dem Kauf
                        else:
                            self.logger.warning(f"Not enough cash to buy shares of {ticker}.")
                    else:
                        self.logger.info(f"Already holding {ticker}, no action needed.")

                elif prediction == 'sell':
                    # Wenn das Modell empfiehlt, die Aktie zu verkaufen
                    if ticker in positions:
                        qty = positions[ticker]
                        self.place_sell_order(ticker, qty)
                        time.sleep(3)
                        account_info = self.get_account_info()  # Aktualisierte Kontoinformationen nach dem Verkauf

                elif prediction == 'hold':
                    # Wenn das Modell empfiehlt, die Aktie zu halten
                    self.logger.info(f"Holding recommendation for {ticker}, no action needed.")


@asset(deps=[setupDVCandVersioningBucketForTraining], group_name="TrainingPhase", compute_kind="LudwigModel")
def trainLudwigModelRegression(context) -> None:
    context.log.info('Trainin Running')
    # Pfade und Werte anpassen
      
    path_ludwig_config = os.getenv("TRAINING_CONFIG_PATH")
    ludwig_config_file_name = os.getenv("TRAINING_CONFIG_NAME")
    data_name = os.getenv("STOCK_NAME")
    model_bucket_url = os.getenv("MODEL_BUCKET")
    mlflow_bucket_url = os.getenv("MLFLOW_BUCKET")
    data_bucket_url = os.getenv("OUTPUT_DIRECTORY")
    model_configs_bucket_url = os.getenv("MODEL_CONFIG_BUCKET")
    
    # Instanz der Klasse erstellen und das Modell trainieren  
    trainer = MLFlowTrainer(ludwig_config_file_name=ludwig_config_file_name, data_name=data_name,
                            ludwig_config_path=path_ludwig_config, model_bucket_url = model_bucket_url,
                            mlflow_bucket_url=mlflow_bucket_url, data_bucket_url=data_bucket_url,
                            model_configs_bucket_url=model_configs_bucket_url)
    trainer.train_model()  
    
    
    subprocess.run(["git", "commit", "-m", "Trainings run from: "+timestampTraining_string+" with data from: "+data+" and the model: "+model+"." ])
    subprocess.run(["git", "push", "-u", "origin", "dev/blue_google_regression"])


##-----------------training area ----------------------------------------------------




@asset(deps=[], group_name="DVCVersioning", compute_kind="DVC")
def setupDVCandVersioningBucket(context) -> None:
    context.log.info('Settings for DVC and S3')
    
    # setup default remote
    timestampTemp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    timestamp=timestampTemp
    
    # Check if all required buckets for the pipeline exist. If not, create them.
    buckets = [os.getenv("OUTPUT_DIRECTORY"), os.getenv("PREDICTIONS_BUCKET"), os.getenv("MODEL_BUCKET"), os.getenv("MLFLOW_BUCKET"), os.getenv("MODEL_CONFIG_BUCKET"), os.getenv("VERSIONING_BUCKET"), os.getenv("VERSIONING_TRAINING_BUCKET"), os.getenv("REPORT_BUCKET"), os.getenv("MLCORE_OUTPUT_RUN")]
    for bucket in buckets:
        
        check_and_create_bucket(bucket)
    
    
    s3_client.put_object(
    Bucket= os.getenv("VERSIONING_BUCKET"),
    Key= timestamp+"/"
    )
    
    subprocess.call(["git", "pull"])
    print("repo is up to date")
    
    subprocess.run(["git", "config", "--global", "user.name", "GlennVerhaag"])
    subprocess.run(["git", "config", "--global", "user.email", "74454853+GlennVerhaag@users.noreply.github.com"])
    subprocess.run(["dvc", "remote", "modify", "versioning", "url", "s3://"+ os.getenv("VERSIONING_BUCKET") + "/" +timestamp])
    subprocess.run(["dvc", "commit"])
    subprocess.run(["dvc", "push"])

    subprocess.run(["git", "add", "../.dvc/config"])
    
  
@asset(deps=[setupDVCandVersioningBucket], group_name="DataCollectionPhase", compute_kind="DVCDataVersioning")
def fetchStockDataFromSource(context) -> None:
    

    #symbols = ['AAPL', 'IBM', 'TSLA', 'NKE', 'AMZN', 'MSFT', 'GOOGL']
    symbols = [os.getenv("STOCK_INPUT")]

    print("Starte den Prozess...")
        
    processed_symbols = []  # Liste, um verarbeitete Symbole zu verfolgen

    for symbol in symbols:
        if symbol not in processed_symbols:  # Überprüfen, ob das Symbol bereits verarbeitet wurde
            print(f"Verarbeite Symbol: {symbol}")
            process_and_upload_symbol_data(
                    api_key='69SMJJ4C2JIW86LI',
                    symbol=symbol,
                    output_directory='data'
                )
            processed_symbols.append(symbol)  # Füge das Symbol zur Liste der verarbeiteten Symbole hinzu
        else:
            print(f"Das Symbol {symbol} wurde bereits verarbeitet, überspringe...")

    print("Prozess abgeschlossen.")


@asset(deps=[fetchStockDataFromSource], group_name="ModelPhase", compute_kind="ModelAPI")
def requestToModel(context):
     
    stock_name = os.getenv("STOCK_NAME")
    file_name = "data_"+stock_name+".csv"
    df = pd.read_csv(f"data/{file_name}")
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
    
    
    os.makedirs("predictionValue", exist_ok=True)
    predictionVariable = response.json()
    prediction_value = predictionVariable['Schluss_predictions']
    context.log.info(f"!!!Prediction ist!!!: {prediction_value}")
    
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
    
    subprocess.run(["dvc", "add", f"predictions/{result_file_name}"])
    print('DVC add successfully')
    subprocess.run(["dvc", "commit"])
    subprocess.run(["dvc", "push"])
    print('DVC push successfully')   
    
    dvcpath= path+".dvc"
    
    subprocess.call(["git", "add", f"{dvcpath}"])
    subprocess.call(["git", "add", "predictions/.gitignore"])
    print("added prediction files to git ")
    
    return prediction_value

    

@asset(deps=[requestToModel], group_name="MonitoringPhase", compute_kind="Reporting")
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
    
    if (len(df.index) > 1): 
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

        os.makedirs("reportings", exist_ok=True)
        reportName=os.getenv("REPORT_NAME")
        reportPath= f"reportings/{reportName}"
        report.run(reference_data=None, current_data=df)
        report.save_html(reportPath)    

        reportsBucket= os.getenv("REPORT_BUCKET")
        path = data_stock+"/"+data_model_version+"/"+reportName

        s3_client.upload_file(reportPath ,reportsBucket, path)      
    
        subprocess.run(["dvc", "add", reportPath])
        print('DVC add successfully')
        subprocess.run(["dvc", "commit"])
        subprocess.run(["dvc", "push"])
        print('DVC push successfully')   
        subprocess.run(["git", "add", "reportings/.gitignore", "reportings/report.html.dvc"])
        print("added reporting files to git ")

    subprocess.run(["git", "commit", "-m", "Pipeline run from "+ date.today().strftime("%d/%m/%Y") +" | Stock: "+ data +" | Model: "+ model ])
    subprocess.run(["git", "push", "-u", "origin", "DagsterPipelineProdRun"])
    context.log.info(subprocess.run(["git", "status"]) )  

    context.log.info(subprocess.run(["git", "log", "--oneline"]) ) 
    
    

@asset(deps=[monitoringAndReporting] ,group_name="StockTrading", compute_kind="Alpacca")
def simulateStockMarket(context, requestToModel):
    # Alpaca API Keys
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL")
    threshold = os.getenv("TRADING_THRESHOLD")
    stockShortform = os.getenv("STOCK_INPUT")
    prediction = requestToModel
    #prediction1 = 50.1

    trader = AlpacaTrader(API_KEY, API_SECRET, BASE_URL, threshold)
    trader.execute_trade(stockShortform, prediction)


##-----------------serving ----------------------------------------------------

@asset(deps=[], group_name="ServingPhase", compute_kind="Serving")
def serviceScript(context) -> None:
    context.log.info("+++++++++++++++++++++++")
    print("##########################")
    ##### Set file/bucket vars #####
    bucket_name = os.environ.get("BUCKET_NAME")
    model_name = os.environ.get("SERVE_MODEL_NAME")
    port = os.environ.get("PORT")
    print(f"bucket_name is {bucket_name}")
    context.log.info(f"bucket_name is {bucket_name}")
    context.log.info(f"model_name is {model_name}")
    s3_client.download_file(bucket_name, f"{model_name}.zip", f"{model_name}.zip")
    with zipfile.ZipFile(f"{model_name}.zip", 'r') as zip_ref:
        zip_ref.extractall(f"./{model_name}".lower())

    # This was originally executed in a script after calling the part top of this
    imagename = model_name.lower()
    create_and_write_serve_model_script()
    create_and_write_serve_model_pyscript()
    create_and_write_dockerfile()
    context.log.info(f"imagename is {imagename}")
    context.log.info(subprocess.run(["docker", "build", "--build-arg", f"model_name={imagename}", "-t", f"{imagename}", "."]))
    container_name: str = ("% s_% s" % (os.getenv("TEAM"), os.getenv("STOCK_NAME")))
    context.log.info(subprocess.run(["./docker", "run", "--name", container_name, "-d", "-p", f"{port}:8000", f"{imagename}"]))



##----------------- trading ----------------------------------------------------

@asset(deps=[], group_name="TradingPhase", compute_kind="Trading")
def tradeScript(context) -> None:
    ##### Set file/bucket vars #####
    # Alpaca API-Schlüssel
    API_KEY = os.getenv("API_KEY", "unset_api_key")
    API_SECRET = os.getenv("API_SECRET", "unset_api_secret")
    BASE_URL = os.getenv("BASE_URL", "unset_base_url")
    threshold = os.getenv("TRADING_THRESHOLD", 0.005)

    # Dictionary von Aktienkürzeln und deren Vorhersagen (sowohl für Regression als auch Klassifikation)
    stocks = {
        'AAPL': 150.0,  # Bei Regression: Vorhergesagter Schlusskurs
        'GOOG': 140.0,  # Bei Klassifikation: Empfehlung ("buy", "sell", "hold")
        'AMZN': 300.0, # Weitere Aktien und deren Vorhersagen hinzufügen
    }

    # Bestimmen des Vorhersagetypen (kann entweder 'regression' oder 'classification' sein)
    prediction_type = os.getenv("PREDICTION_TYPE", "unset_prediction_type")  # Ändern je nach verwendetem Modell

    trader = AlpacaTrader(API_KEY, API_SECRET, BASE_URL, threshold, stocks, context, prediction_type)
    trader.execute_trade()
