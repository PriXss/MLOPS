import subprocess
import sys
import os

def install(package):
    subprocess.run([sys.executable, "-m", "pip", "install", package], check=True)

# install minio package to access
# install("minio")

import boto3


def main():
  s3 = boto3.resource('s3',
    endpoint_url='http://172.23.0.2:9000',
    aws_access_key_id='aIiLEEiXspwTutA8WLTD',
    aws_secret_access_key='QRLlCyF1tzLl8kAi7WtAc1LNbYOdBtf9GKtS9BDV',
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False
  )

  bucket_name = os.environ.get("BUCKET_NAME","models")
  model_name = os.environ.get("MODEL_NAME", "deep_lstm")
  print(f"bucket_name is {bucket_name}")
  print(f"model_name is {model_name}")
  bucket = s3.Bucket(bucket_name)
  for obj in bucket.objects.filter(Prefix=model_name):
    target = obj.key
    if not os.path.exists(os.path.dirname(target)):
      os.makedirs(os.path.dirname(target))
    if obj.key[-1] == '/':
      continue
    bucket.download_file(obj.key, target)

main()
