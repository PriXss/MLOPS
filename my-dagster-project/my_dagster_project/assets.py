
from dagster import asset
import boto3
from botocore.config import Config
import pandas as pd
import subprocess

@asset(group_name="data_collection", compute_kind="DataExtraction" )
def getRawDataFromBucket() -> None:
    s3 = boto3.resource(
        endpoint_url= 'http://85.215.53.91:9000',
        aws_secret_key_id= 'TqDMLOXxw2OipC6Tp0mv',
        aws_secret_access_key='l1d0Elk1bH7X3bB2NOIXHOwGIJJSPcZvD9MLWQ58',
        config= Config(signature_version='s3v4'),
        region_name='us-east-1',
    )
    s3.Bucket('data').download_file('siemens.csv', 'siemens.csv')
    
@asset(deps=[getRawDataFromBucket] ,group_name="data_versioning", compute_kind="DataVersioning" )
def version_with_DVC_Container() -> None:
    subprocess.run(['dvc', 'add', 'siemens.csv'])
    subprocess.run(['dvc', 'commit'])
    subprocess.run(['git', 'add' '.'])
    subprocess.run(['git', 'commit', '-m', 'Added new siemens data'])



    


