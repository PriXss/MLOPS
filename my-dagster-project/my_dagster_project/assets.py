import os
import csv
from dagster import asset
import subprocess

@asset(group_name="data_collection", compute_kind="DataExtraction" )
def callDataCollectionScript():
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
def version_with_DVC_Container() -> None:
    subprocess.run(['cd', '/data/siemens/'])
    subprocess.run(['dvc', 'add', 'siemens.csv'])
    subprocess.run(['dvc', 'commit'])
    subprocess.run(['git', 'add' '.'])
    subprocess.run(['git', 'commit', '-m', 'Added new siemens data'])