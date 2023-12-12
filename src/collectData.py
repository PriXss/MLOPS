import pandas as pd
import os as os
import csv as csv



print("Datensatz wurde collected und kann im ML Prozess verwendet werden")

data_path = os.path.join('data', 'collected')
os.makedirs(data_path, exist_ok=True)

df= pd.read_csv(r"./test.csv")

print(df)

