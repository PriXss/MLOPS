import pandas as pd
import os as os
import csv as csv


data_path = os.path.join('data', 'collected')
os.makedirs(data_path, exist_ok=True)


dataframe = pd.read_csv('test.csv')

dataframe.to_csv('collected.csv')

print("Datensatz wurde collected und kann im ML Prozess verwendet werden")


