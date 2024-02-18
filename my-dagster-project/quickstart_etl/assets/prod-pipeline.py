import os
import subprocess
import pandas as pd
import boto3
from dagster import asset





@asset(group_name="DataCollectionPhase", compute_kind="DVCDataVersioning")
def fetchStockDataFromSource(context) -> None:
    context.log.info('Could Retrieve the Data from the API and store it in S3 accordingly')





@asset(deps=[fetchStockDataFromSource] ,group_name="DataCollectionPhase", compute_kind="S3DataCollection")
def getStockData(context) -> None:
    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        aws_access_key_id='test',
        aws_secret_access_key='testpassword',
        endpoint_url='http://85.215.53.91:9000',
    )
    bucket = "data"
    file_name = "data.csv"
    obj = s3_client.get_object(Bucket= bucket, Key= file_name) 
    initial_df = pd.read_csv(obj['Body'])
    context.log.info('Data Extraction complete')
    context.log.info(initial_df.head())
    os.makedirs("data", exist_ok=True)
    initial_df.to_csv('data/stocks.csv', index=False)        






@asset(deps=[getStockData], group_name="VersioningPhase", compute_kind="DVCDataVersioning")
def versionStockData(context) -> None:
    subprocess.run(["dvc", "add", "data/stocks.csv"])
    subprocess.run(["git", "add", "data/stocks.csv.dvc"])
    subprocess.run(["git", "add", "data/.gitignore"])
    subprocess.run(["git", "commit", "-m", "Add new Data for Prod Runs"])
    subprocess.run(["dvc", "push"])
    context.log.info('Data successfully versioned')






@asset(deps=[getStockData], group_name="ModelPhase", compute_kind="ModelAPI")
def requestToModel(context) -> None:
    subprocess.run(["dvc", "pull"])
    #get the data to processs with the model
    #send api request to the model wait for successful request
    context.log.info('Requesting to Model was succesful')

    
    
    
    
    
    
    
@asset(deps=[requestToModel], group_name="ModelPhase", compute_kind="ModelAPI")
def getPredicition(context) -> None:
    #await for a response from the model api
    #store the data into git or s3 to get retrieved later
    context.log.info('Predicition successfully recieved from the Model')





@asset(deps=[getPredicition], group_name="VersioningPhase", compute_kind="DVCDataVersioning")
def versionPrediction(context) -> None:
    #check the output sorce s3 or git, grab the data and version it to the input data
    context.log.info('Prediction could be versioned accordingly')
    


@asset(deps=[getPredicition], group_name="VersioningPhase", compute_kind="DVCDataVersioning")
def versioningModel(context) -> None:
    context.log.info('I can identify which model is served atm and have a reference to it accordingly')
