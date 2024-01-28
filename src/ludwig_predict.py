import requests

url = "http://localhost:8000/predict"

data = {
    "data": [
        {"Tag": 15, "Monat": 12, "Jahr": 2023},
        {"Tag": 20, "Monat": 1, "Jahr": 2023}
    ]
}

response = requests.post(url, json=data, params={"model_path": "..\models\Prognose_Kurs"})
predictions = response.json()["predictions"]

for prediction in predictions:
    predicted_closing_price = prediction["Schluss"]
    print("Vorhergesagter Schlusskurs:", predicted_closing_price)
