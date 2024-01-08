import pandas as pd

def transform_date_to_numerical(input_csv, output_csv):
    # Lese die CSV-Datei
    data = pd.read_csv(input_csv)

    # Konvertiere das Datum in das Datetime-Format unter Angabe des erwarteten Formats
    data['Datum'] = pd.to_datetime(data['Datum'], format='%y-%m-%d')

    # Extrahiere den Tag, den Monat und das Jahr als separate Spalten
    data['Tag'] = data['Datum'].dt.day
    data['Monat'] = data['Datum'].dt.month
    data['Jahr'] = data['Datum'].dt.year

    # Speichere das transformierte DataFrame in einer neuen CSV-Datei
    data.to_csv(output_csv, index=False)

if __name__ == "__main__":
    input_csv = "../data/data.csv"
    output_csv = "../data/transformierte_datei.csv"

    transform_date_to_numerical(input_csv, output_csv)
