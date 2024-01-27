
from dagster import asset 
import boto3
from botocore.config import Config

@asset()
def getRawData(context):
    
    s3 = boto3.resource(
        endpoint_url= 'http://85.215.53.91:9000',
        aws_secret_key_id= 'TqDMLOXxw2OipC6Tp0mv',
        aws_secret_access_key='l1d0Elk1bH7X3bB2NOIXHOwGIJJSPcZvD9MLWQ58',
        config= Config(signature_version='s3v4'),
        region_name='us-east-1',
    )
    s3.Bucket('data').download_file('siemens.csv', 'siemens.csv')
    return



