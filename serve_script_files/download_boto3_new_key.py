import subprocess
import sys
import os
import zipfile

def install(package):
    subprocess.run([sys.executable, "-m", "pip", "install", package], check=True)

# install minio package to access
# install("minio")

import boto3


def main():

  session = boto3.session.Session()
  s3_client = session.client(
    service_name= "s3",
    aws_access_key_id="test",
    aws_secret_access_key="testpassword",
    endpoint_url="http://85.215.53.91:9000",
  )


  s3 = boto3.resource('s3',
    endpoint_url='http://172.23.0.2:9000',
    aws_access_key_id='aIiLEEiXspwTutA8WLTD',
    aws_secret_access_key='QRLlCyF1tzLl8kAi7WtAc1LNbYOdBtf9GKtS9BDV',
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False
  )

  bucket_name = os.environ.get("BUCKET_NAME")
  model_name = os.environ.get("MODEL_NAME")
  print(f"bucket_name is {bucket_name}")
  print(f"model_name is {model_name}")
  s3_client.download_file(bucket_name, f"{model_name}.zip", f"{model_name}.zip")
  with zipfile.ZipFile(f"{model_name}.zip", 'r') as zip_ref:
    zip_ref.extractall(f"./{model_name}".lower())
  
main()
