from ludwig.api import LudwigModel
import matplotlib.pyplot as plt

# Pfade zu den Daten und der Konfigurationsdatei
csv_file_path = ':/testing_data_extraction/data_Google.csv'
config_file_path = './model_config_files/ludwig_MLCore.yaml'

model = LudwigModel(config_file_path)

# Trainiere das Modell mit dem CSV-Datensatz
train_stats = model.train(dataset=csv_file_path)

# Extrahieren der Verluste und Genauigkeiten
train_loss = train_stats['training']['combined']['loss']
val_loss = train_stats['validation']['combined']['loss']
train_accuracy = train_stats['training']['combined']['accuracy']
val_accuracy = train_stats['validation']['combined']['accuracy']

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