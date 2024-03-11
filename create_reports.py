##### Imports #####
import pandas as pd
import numpy as np
import os
import boto3
import botocore
import subprocess
import json
import requests
from datetime import datetime
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, DataQualityPreset, RegressionPreset
from evidently.metrics import *
from evidently.tests import *

session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id='test',
    aws_secret_access_key='testpassword',
    endpoint_url='http://85.215.53.91:9000',
)
bucket = "data"
file_name = "data_Amazon.csv"
obj = s3_client.get_object(Bucket= bucket, Key= file_name) 
initial_df = pd.read_csv(obj['Body'])
os.makedirs("prepareModelRequest", exist_ok=True)
initial_df.to_csv('prepareModelRequest/stocks.csv', index=False)  

headers = {'User-Agent': 'Mozilla/5.0'}
payload = {
    "Datum": "21.02.2024",
    "Eroeffnung": "150.11",
    "Tageshoch": "155.19",
    "Tagestief": "149.14",
    "Schluss": "155.11",
    "Umsatz":"43390126.0",
    "RSI": "41.6",
    "EMA": "145.5158"
    }

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
file_name = "Predictions_Google_finalModel.csv"
session = requests.Session()
response= session.post('http://85.215.53.91:8002/predict',headers=headers,data=payload)
os.makedirs("predictions", exist_ok=True)
resultPayload = [payload, response.json()]

resultJson = {**payload, **response.json()}
        
df_result = pd.DataFrame(resultJson, index=[0])

try:
    s3_client.head_object(Bucket='predictions', Key=file_name)
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        # Prediction-file for stock/model combination doesn't yet exist in the s3 bucket
        df_result.to_csv(f'predictions/{file_name}', mode='w', index=False, header=True) # Create prediction-file (incl. header)
    else:
        # Other error
        print("error!")
        # context.log.info('Connection to S3-bucket failed!')
else:
    # Prediction-file exists
    s3_client.download_file('predictions', file_name, 'predictions/'+file_name) # Download prediction-file to disk
    df_result.to_csv(f'predictions/{file_name}', mode='a', index=False, header=False) # Append prediction file (excl. header)
    

bucket = "predictions"
s3_client.upload_file(f'predictions/{file_name}', bucket, file_name)

##### Ignore warnings #####
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

##### Set file/bucket vars #####
data_bucket_url = "predictions"
data_stock = "Google"
data_model_version = "finalModel"

##### Set access vars #####
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testpassword"
os.environ["AWS_ENDPOINT_URL"] = "http://85.215.53.91:9000"

##### Load data from the bucket #####
s3 = boto3.client('s3')
obj = s3.get_object(Bucket=data_bucket_url, Key= "Predictions_"+data_stock+"_"+data_model_version+".csv" )
df = pd.read_csv(obj['Body'])

##### Data prep #####
df = df.rename(columns={'Schluss': 'target', 'Schluss_predictions': 'prediction'}) # Rename columns to fit evidently input
df['prediction'] = df['prediction'].shift(1) # Shift predictions to match them with the actual target (close price of the following day)
df = df.iloc[1:] # drop first row, as theres no matching prediction 

##### Create report #####
#Reference-Current split
reference = df.iloc[int(len(df.index)/2):,:]
current = df.iloc[:int(len(df.index)/2),:]

report = Report(metrics=[
    DataDriftPreset(), 
    TargetDriftPreset(),
    DataQualityPreset(),
    RegressionPreset()
])

report.run(reference_data=reference, current_data=current)
report.save_html("report.html")

##### Upload report to bucket #####
s3 = boto3.resource('s3')
obj = s3.Object("reports", "/"+data_stock+"/"+data_model_version+"/report.html")
obj.put(Body=open('report.html', 'rb'))