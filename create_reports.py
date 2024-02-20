##### Imports #####
import pandas as pd
import numpy as np
import os
import boto3

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, DataQualityPreset, RegressionPreset
from evidently.metrics import *
from evidently.tests import *

##### Ignore warnings #####
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

##### Set file/bucket vars #####
data_bucket_url = "logs"
data_stock = "IBM"
data_model_version = "model2"
data_date = "20022024"

##### Set access vars #####
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testpassword"
os.environ["AWS_ENDPOINT_URL"] = "http://85.215.53.91:9000"

##### Load data from the bucket #####
s3 = boto3.client('s3')
obj = s3.get_object(Bucket=data_bucket_url, Key= data_date+"_"+data_stock+"_"+data_model_version+".csv" )
df = pd.read_csv(obj['Body'])

##### Create report #####
reference = df.iloc[50:,:]
current = df.iloc[:50,:]

report = Report(metrics=[
    DataDriftPreset(), 
    TargetDriftPreset(),
    RegressionPreset()
])

report.run(reference_data=reference, current_data=current)
report.save_html("report.html")

##### Upload report to bucket #####
s3 = boto3.resource('s3')
obj = s3.Object("reports", "/"+data_stock+"/"+data_model_version+"/report.html")
obj.put(Body=open('report.html', 'rb'))