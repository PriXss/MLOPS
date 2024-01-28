import os
import csv
from dagster import asset
import subprocess

@asset(group_name="data_collection", compute_kind="DataExtraction" )
def callDataCollectionScript() -> None:
    """This script is used to run the data collection script"""
    # Replace 'path/to/directory' with the path where you want to create the directory
    os.mkdir('/data/siemens/')

    # Replace 'filename.csv' with the name of the CSV file you want to create
    filename = 'siemens.csv'

    # Replace 'path/to/directory' with the path of the directory you created earlier
    filepath = os.path.join('/data/siemens/', filename)

    # Create a new CSV file in the directory
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['aktienName', 'Siemens'])
        writer.writerow(['Preis', '100€'])

    
@asset(deps=[callDataCollectionScript] ,group_name="data_versioning", compute_kind="DataVersioning" )
def version_with_DVC() -> None:
    subprocess.run(['cd', 'data/siemens/'])
    subprocess.run(['dvc', 'add', 'siemens.csv'])
    subprocess.run(['dvc', 'commit'])
    subprocess.run(['dvc', 'push'])
    subprocess.run(['git', 'add' '.'])
    subprocess.run(['git', 'commit', '-m', 'Added new siemens data'])


@asset(group_name="ProvideModell", compute_kind="ModelServing" )
def provideAndVersionMLModell() -> None:
    # try to sping up the model container here or make a reference to the model that is beeing used
    #track the model with dvbc as well, TBD how modell serving looks like
    pass


@asset(deps=[version_with_DVC, provideAndVersionMLModell ] ,group_name="DataToModell", compute_kind="DataProcessing" )
def processDataFromDVCStorageWithModell() -> None:
    subprocess.run(['dvc', 'pull'])
    ##call the model container and pass the data to the container
    ## recieve the results from ML modell and safe it as 
    os.mkdir('/resultData')
    filename = 'siemensResults.csv'

    # Replace 'path/to/directory' with the path of the directory you created earlier
    filepath = os.path.join('/resultData/', filename)
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Prognose', 'kaufen'])
    ##store results in resultData as results.csv
    subprocess.run(['dvc', 'add', 'resultData/siemensResults.csv'])
    subprocess.run(['dvc', 'commit'])
    subprocess.run(['dvc', 'push'])
    subprocess.run(['git', 'add' '.'])
    subprocess.run(['git', 'commit', '-m', 'Added siemens result data'])

