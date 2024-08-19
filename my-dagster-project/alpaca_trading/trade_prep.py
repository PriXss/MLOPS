import os
import csv
import boto3
import pandas as pd
import json


class CSVProcessor:
    def __init__(self, bucket_name, local_download_path, minio_config, stock_symbol_mapping):
        self.bucket_name = bucket_name
        self.local_download_path = local_download_path
        self.minio_client = boto3.client(
            's3',
            endpoint_url=minio_config['endpoint_url'],
            aws_access_key_id=minio_config['aws_access_key_id'],
            aws_secret_access_key=minio_config['aws_secret_access_key']
        )
        self.stock_symbol_mapping = stock_symbol_mapping
        os.makedirs(self.local_download_path, exist_ok=True)

    def download_files(self):
        response = self.minio_client.list_objects_v2(Bucket=self.bucket_name)
        downloaded_files = []

        for obj in response.get('Contents', []):
            file_name = os.path.basename(obj['Key'])
            local_file_path = os.path.join(self.local_download_path, file_name)
            self.minio_client.download_file(self.bucket_name, obj['Key'], local_file_path)
            downloaded_files.append(local_file_path)

        return downloaded_files

    def extract_last_row(self, file_path):
        df = pd.read_csv(file_path)

        # Filtere nur die Spalten, die wir behalten wollen
        columns_to_keep = ['Schluss_predictions']
        df_filtered = df[columns_to_keep]

        # Extrahiere die letzte Zeile
        last_row = df_filtered.iloc[-1]

        file_name = os.path.basename(file_path)
        stock_name = self.get_stock_name_from_filename(file_name)
        stock_symbol = self.stock_symbol_mapping.get(stock_name, 'UNKNOWN')

        last_row_list = last_row.tolist()
        last_row_list.append(stock_symbol)

        return last_row_list, columns_to_keep + ['Stock Symbol']

    def get_stock_name_from_filename(self, file_name):
        parts = file_name.split('_')
        return parts[1] if len(parts) > 1 else None

    def process_files(self, downloaded_files):
        final_data = []
        columns = None

        for file_path in downloaded_files:
            last_row, cols = self.extract_last_row(file_path)
            final_data.append(last_row)
            if not columns:
                columns = cols

        return final_data, columns

    def save_to_csv(self, final_data, columns, output_file):
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(final_data)

        print(f'Die extrahierten Daten wurden in {output_file} gespeichert.')

    def create_stocks_dict(self, final_data):
        stocks = {}
        for row in final_data:
            stock_symbol = row[-1]  # Das letzte Element ist das Stock Symbol
            prediction = row[0]  # Die Vorhersage (Schlusskurs oder Klassifikation)
            stocks[stock_symbol] = prediction
        return stocks

    def save_stocks_to_json(self, stocks, json_output_file):
        with open(json_output_file, 'w') as json_file:
            json.dump(stocks, json_file, indent=4)
        print(f'Die Aktienvorhersagen wurden in {json_output_file} gespeichert.')

    def cleanup_files(self, files_to_delete):
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f'{file_path} wurde gelöscht.')

    def cleanup_directory(self):
        for file_name in os.listdir(self.local_download_path):
            file_path = os.path.join(self.local_download_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(self.local_download_path)
        print(f'Alle Dateien in {self.local_download_path} wurden gelöscht und das Verzeichnis wurde entfernt.')


if __name__ == '__main__':
    # Konfiguration
    minio_config = {
        'endpoint_url': 'http://85.215.53.91:9000',
        'aws_access_key_id': 'test',
        'aws_secret_access_key': 'testpassword'
    }

    stock_symbol_mapping = {
        'Google': 'GOOGL',
        'Apple': 'AAPL',
        'IBM': 'IBM',
        'SAP': 'SAP',
        'Tesla': 'TSLA',
        'Amazon': 'AMZN'
        # Weitere Zuordnungen hier hinzufügen
    }

    bucket_name = 'predictions'
    local_download_path = './downloaded_csvs/'
    output_file = 'arr_predictions.csv'
    json_output_file = 'stocks_predictions.json'

    # CSVProcessor-Instanz erstellen
    processor = CSVProcessor(bucket_name, local_download_path, minio_config, stock_symbol_mapping)

    # Dateien herunterladen
    downloaded_files = processor.download_files()

    # Letzte Zeilen extrahieren und verarbeiten
    final_data, columns = processor.process_files(downloaded_files)

    # Daten in eine neue CSV-Datei schreiben
    processor.save_to_csv(final_data, columns, output_file)

    # Stocks Dictionary erstellen
    stocks = processor.create_stocks_dict(final_data)

    # Stocks Dictionary in eine JSON-Datei speichern
    processor.save_stocks_to_json(stocks, json_output_file)

    # Heruntergeladene Dateien und CSV-Datei löschen
    processor.cleanup_files(downloaded_files + [output_file])

    # Download-Verzeichnis bereinigen
    processor.cleanup_directory()