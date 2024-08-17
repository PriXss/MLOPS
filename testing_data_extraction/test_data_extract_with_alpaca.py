import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame
import os

# Alpaca API-Schlüssel und -Secret
API_KEY = 'PKBLYQY5KJOB2A3IT4V5'
API_SECRET = 'a2S1o0VsDm5VPMvLLVgUAQpS03YOujLmj9OBiJha'
BASE_URL = 'https://paper-api.alpaca.markets'  # Verwende die URL für den Papierhandel oder den Live-Handel

# Alpaca API-Client erstellen
api = REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

def get_all_historical_data(symbol, start_date, end_date):
    """Holt alle historischen Daten für das gegebene Symbol zwischen den angegebenen Daten."""
    all_data = []
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    while start_date < end_date:
        next_date = start_date + pd.DateOffset(days=1000)  # Maximale Zeitspanne für eine Anfrage
        if next_date > end_date:
            next_date = end_date

        data = api.get_bars(
            symbol,
            TimeFrame.Day,
            start=start_date.strftime('%Y-%m-%d'),
            end=next_date.strftime('%Y-%m-%d')
        ).df

        if not data.empty:
            all_data.append(data)
        start_date = next_date + pd.DateOffset(days=1)  # Überspringen des Enddatums des aktuellen Intervalls

    full_data = pd.concat(all_data).drop_duplicates()
    return full_data


def process_and_upload_symbol_data(symbol, start_date, end_date):
    try:
        # Holen Sie sich alle historischen Kursdaten
        data = get_all_historical_data(symbol, start_date, end_date)

        if data.empty:
            print(f"Keine Daten für {symbol} abgerufen.")
            return

        # Debug-Ausgabe: Überprüfe die Struktur der Daten
        print("Erste Zeilen der Daten:")
        print(data.head())
        print("Spalten der Daten:")
        print(data.columns)

        # Sicherstellen, dass der Index als Datum formatiert ist
        data.index = pd.to_datetime(data.index)

        # Konvertiere den 'timestamp'-Index in eine Spalte 'Datum'
        data.reset_index(inplace=True)
        data.rename(columns={'index': 'Datum', 'timestamp': 'Datum'}, inplace=True)

        # Formatieren des Datums als YYYY-MM-DD und als Datentyp date
        data['Datum'] = pd.to_datetime(data['Datum']).dt.date

        # Entfernen von Duplikaten
        data.drop_duplicates(inplace=True)

        # Umbenennen der Spalten auf Deutsch
        data.rename(columns={
            'close': 'Schluss',
            'high': 'Tageshoch',
            'low': 'Tagestief',
            'trade_count': 'Handelsanzahl',
            'open': 'Eroeffnung',
            'volume': 'Umsatz',
            'vwap': 'VWAP'
        }, inplace=True)

        # Berechnen der technischen Indikatoren
        data['RSI'] = data['Schluss'].rolling(window=14).apply(lambda x: pd.Series(x).diff().gt(0).sum() / 14.0)
        data['EMA'] = data['Schluss'].ewm(span=10, adjust=False).mean()
        data['SMA'] = data['Schluss'].rolling(window=10).mean()
        data['DEMA'] = 2 * data['EMA'] - data['EMA'].ewm(span=10, adjust=False).mean()

        # Berechnung der Differenz und Hinzufügen der neuen Spalten
        data['Differenz'] = data['Schluss'] - data['Eroeffnung']
        data['Status'] = data['Differenz'].apply(lambda x: 'gestiegen' if x > 0 else 'gefallen')
        data['Empfehlung'] = data['Status'].apply(lambda x: 'kaufen' if x == 'gestiegen' else 'verkaufen')
        data['Schluss naechster Tag'] = data['Schluss'].shift(-1)

        # Sortieren des DataFrames nach dem Datum in aufsteigender Reihenfolge
        data_sorted = data.sort_values(by='Datum')

        # Filter the data for dates before or equal to End-Date
        data_2023 = data_sorted[data_sorted['Datum'] <= pd.to_datetime(end_date).date()]

        # Display the first few rows of the filtered dataframe
        print("Gefilterte Daten bis zum End_date:")
        print(data_2023.head())

        # Quality Checks vor dem Sortieren und Speichern
        if not pd.to_datetime(data_sorted['Datum'], errors='coerce').notnull().all():
            print(f"Warnung: Einige Datumsangaben für {symbol} sind nicht im erwarteten Format.")

        neuestes_datum = data_sorted['Datum'].max()
        grenzwerte = {
            'Eroeffnung': (1, 5000),
            'Tageshoch': (1, 5000),
            'Tagestief': (1, 5000),
            'Schluss': (1, 5000),
        }
        neueste_zeile = data_sorted[data_sorted['Datum'] == neuestes_datum]

        if neueste_zeile.empty:
            print(f"Keine Daten für das neueste Datum ({neuestes_datum}) gefunden.")
            return

        if not pruefe_extreme_werte(neueste_zeile.iloc[0], grenzwerte):
            print(f"Warnung: Extreme Werte für {symbol} gefunden. Upload der Datei wird abgelehnt.")
            upload_abgelehnt = True
        else:
            upload_abgelehnt = False

        werte_zu_pruefen = ['Eroeffnung', 'Tageshoch', 'Tagestief', 'Schluss']
        for spalte in werte_zu_pruefen:
            if neueste_zeile[spalte].iloc[0] == 0:
                print(f"Der neueste Wert für '{spalte}' ist 0. Upload der Datei wird abgelehnt.")
                upload_abgelehnt = True
                break

        # Speichern der Daten als CSV
        csv_filepath = os.path.join('../testing_data_extraction', f'data_{symbol}.csv')
        data_2023.to_csv(csv_filepath, index=False)
        print(f"Datei gespeichert unter: {csv_filepath}")

    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")


def pruefe_extreme_werte(reihe, grenzwerte):
    for spalte, (min_wert, max_wert) in grenzwerte.items():
        if reihe[spalte] < min_wert or reihe[spalte] > max_wert:
            return False
    return True


if __name__ == "__main__":
    start_date = '2023-01-01'
    end_date = '2024-08-16'
    process_and_upload_symbol_data('GOOGL', start_date, end_date)