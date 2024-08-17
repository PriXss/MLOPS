import pandas as pd
import matplotlib.pyplot as plt

# Lade die CSV-Datei
df = pd.read_csv('../testing_data_extraction/data_GOOGL.csv')

# Überprüfe, ob die Spalte 'Schluss' vorhanden ist
if 'Schluss' in df.columns:
    # Sicherstellen, dass die Datumsspalte als Datetime-Objekte vorliegt
    df['Datum'] = pd.to_datetime(df['Datum'], format='%Y-%m-%d')
    df.sort_values(by='Datum', inplace=True)

    # Visualisiere die Daten der Spalte 'Schluss'
    plt.figure(figsize=(12, 6))
    plt.plot(df['Datum'], df['Schluss'], label='Schlusskurs', color='blue')
    plt.ylabel('Schlusskurs')
    plt.title('Schlusskurs über die Zeit')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
else:
    print("Die Spalte 'Schluss' ist nicht in den Daten vorhanden.")
