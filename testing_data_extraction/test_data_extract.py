import os
import pandas as pd
import boto3
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators

session = boto3.session.Session()
s3_client = session.client(
    service_name= "s3",
    aws_access_key_id= "test",
    aws_secret_access_key="testpassword",
    endpoint_url='http://85.215.53.91:9000',
)

def pruefe_extreme_werte(reihe, grenzwerte):
    for spalte, (min_wert, max_wert) in grenzwerte.items():
        if reihe[spalte] < min_wert or reihe[spalte] > max_wert:
            return False  # Wert liegt außerhalb der Grenzen
    return True  # Alle Werte liegen innerhalb der Grenzen

def process_and_upload_symbol_data(symbol, api_key):
    # Definieren der Alpha Vantage Objekte im Pandas Format
    ts = TimeSeries(key=api_key, output_format='pandas')
    ti = TechIndicators(key=api_key, output_format='pandas')

    company_names = {
        'AAPL': 'Apple', 'IBM': 'IBM', 'TSLA': 'Tesla', 'NKE': 'Nike',
        'AMZN': 'Amazon', 'MSFT': 'Microsoft', 'GOOGL': 'Google'
    }

    company_name = company_names.get(symbol, 'Company')
    csv_filename = f'data_{company_name}.csv'
    csv_filepath = '../testing_data_extraction'

    print(f"{csv_filename}")

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

    # Berechnung der Differenz und Hinzufügen der neuen Spalten
    print("Füge Differenz hinzu")
    merged_data['Differenz'] = merged_data['Schluss'] - merged_data['Eroeffnung']
    print("Füge Status hinzu")
    merged_data['Status'] = merged_data['Differenz'].apply(lambda x: 'gestiegen' if x > 0 else 'gefallen')

    merged_data['Empfehlung'] = merged_data['Status'].apply(lambda x: 'kaufen' if x == 'gestiegen' else 'verkaufen')
    print("Füge nächster Schlusskurs hinzu")
    merged_data['Schlusskurs nächster Tag'] = merged_data['Schluss'].shift(+1)

    # Sortieren des zusammengeführten DataFrame nach dem Datum in aufsteigender Reihenfolge
    merged_data_sorted = merged_data.sort_values(by='Datum', ascending=True)

    # Convert the 'Datum' column to datetime format
    merged_data_sorted['Datum'] = pd.to_datetime(merged_data_sorted['Datum'])

    # Filter the data for dates before or equal to 31.12.2023
    data_2023 = merged_data_sorted[merged_data_sorted['Datum'] <= '2023-12-31']

    # Display the first few rows of the filtered dataframe
    print(data_2023.head())

    # Quality Checks vor dem Sortieren und Speichern

    # Überprüfen, ob die Datumsangaben im richtigen Format vorliegen
    if not pd.to_datetime(merged_data_sorted['Datum'], errors='coerce').notnull().all():
        print(f"Warnung: Einige Datumsangaben für {symbol} sind nicht im erwarteten Format.")

    # Sicherstellen, dass der Wert für das neueste Datum aktualisiert wird
    neuestes_datum = merged_data_sorted['Datum'].max()

    # Definition der Grenzwerte
    grenzwerte = {
        'Eroeffnung': (1, 5000),
        'Tageshoch': (1, 5000),
        'Tagestief': (1, 5000),
        'Schluss': (1, 5000),
    }
    # Prüfen auf extreme Werte für den neuesten Datensatz
    neueste_zeile = merged_data_sorted[merged_data_sorted['Datum'] == neuestes_datum]
    if not pruefe_extreme_werte(neueste_zeile.iloc[0], grenzwerte):
        print(f"Warnung: Extreme Werte für {symbol} gefunden. Upload der Datei wird abgelehnt.")
        upload_abgelehnt = True
    else:
        upload_abgelehnt = False  # Setzen Sie upload_abgelehnt nur dann auf False, wenn die Überprüfungen bestanden sind

    # Quality Check für fehlende Werte

    # Überprüfen der Werte für 'Eroeffnung', 'Tageshoch', 'Tagestief' und 'Schluss' am neuesten Datum
    werte_zu_pruefen = ['Eroeffnung', 'Tageshoch', 'Tagestief', 'Schluss']
    for spalte in werte_zu_pruefen:
        if neueste_zeile[spalte].iloc[0] == 0:
            print(f"Der neueste Wert für '{spalte}' ist 0. Upload der Datei wird abgelehnt.")
            upload_abgelehnt = True
            break  # Beendet die Schleife, da mindestens ein Wert 0 ist

    # Sortierten DataFrame als CSV exportieren
    csv_filepath = os.path.join(csv_filepath, csv_filename)
    data_2023.to_csv(csv_filepath, index=False)

if __name__ == "__main__":
    process_and_upload_symbol_data('GOOGL', '69SMJJ4C2JIW86LI')
