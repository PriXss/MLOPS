from ludwig.api import LudwigModel
import matplotlib.pyplot as plt

# Pfade zu den Daten und der Konfigurationsdatei
csv_file_path = '../testing_data_extraction/data_GOOGL.csv'
config_file_path = '../model_config_files/ludwig_MLCore.yaml'

# Initialisiere das Ludwig Modell
model = LudwigModel(config_file_path)
print("Starte Training")

# Trainiere das Modell mit dem CSV-Datensatz
train_stats = model.train(dataset=csv_file_path)

# Extrahieren der Verluste und Genauigkeiten
# Überprüfen, ob 'combined' und 'loss' in train_stats existieren
if 'training' in train_stats and 'combined' in train_stats['training']:
    train_loss = train_stats['training']['combined'].get('loss', [])
    val_loss = train_stats.get('validation', {}).get('combined', {}).get('loss', [])
    train_accuracy = train_stats['training']['combined'].get('accuracy', [])
    val_accuracy = train_stats.get('validation', {}).get('combined', {}).get('accuracy', [])
else:
    print("Fehlende Trainingsstatistiken.")
    train_loss = []
    val_loss = []
    train_accuracy = []
    val_accuracy = []

# Visualisierung der Verluste
plt.figure(figsize=(12, 6))

plt.subplot(1, 2, 1)
plt.plot(train_loss, label='Train Loss')
plt.plot(val_loss, label='Validation Loss')
plt.title('Loss over Epochs')
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.legend()

# Visualisierung der Genauigkeiten
plt.subplot(1, 2, 2)
plt.plot(train_accuracy, label='Train Accuracy')
plt.plot(val_accuracy, label='Validation Accuracy')
plt.title('Accuracy over Epochs')
plt.xlabel('Epoch')
plt.ylabel('Accuracy')
plt.legend()

plt.show()

# Optional: Speichern des trainierten Modells
model.save('ludwig_model')

# Optional: Vorhersagen auf neuen Daten
predictions = model.predict(dataset=csv_file_path)
