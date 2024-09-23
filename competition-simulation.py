import pandas as pd
import os
import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

session = boto3.session.Session()
s3_client = session.client(
    service_name= "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url="http://85.215.53.91:9000",
    )
sessionRequest = requests.Session()

startdate = "2024-01-02" # Stock market closed on 01.01., so were using 02.01.
timeperiod = 40 # in days
stocks = {
    "Google":["http://85.215.53.91:8041/predict"],
    #"IBM":["http://85.215.53.91:8043/predict"],
    "Apple" : ["http://85.215.53.91:8095/predict"],
    #"Tesla":[],
    #"SAP":[],
    #"Amazon:[]"
} 

dataframes = {}
expected_profits= {}
errors = {}
gain_or_loss_correct_predictions = {} 

def determine_correct_gain_loss_predictions(stock, model):
        print(stock + ", Model " + str(model+1) + ":  \n Predicted Gain/Loss: " + str(round(expected_profit,2)) + "\n Actual Gain/Loss: " + str(round(actual_gain_or_loss,2)))
        if (expected_profit < 0 and actual_gain_or_loss < 0):
            gain_or_loss_correct_predictions[stock][model] += 1 
        elif expected_profit > 0 and actual_gain_or_loss > 0:
            gain_or_loss_correct_predictions[stock][model] += 1
        elif expected_profit == 0 and actual_gain_or_loss == 0:
            gain_or_loss_correct_predictions[stock][model] += 1          
 
print("\n Starting simulation...")
                
for days in range(timeperiod):
    print("\n Day " + str(days+1) + ":")
    for stock in stocks:
        obj = s3_client.get_object(Bucket="data", Key= "data_"+stock+".csv" )
        dataframes[stock]=(pd.read_csv(obj['Body']))
        df = dataframes[stock]
        first_index=df.index[df['Datum'] == startdate]

        payload = df.iloc[first_index+days].to_dict('list')
        
        if stock not in gain_or_loss_correct_predictions:
            gain_or_loss_correct_predictions[stock] = [0]
        
        for model in range(len(stocks[stock])):

            raw_response= sessionRequest.post(stocks[stock][model],data=payload) 
            response = raw_response.json()['Schluss_predictions']
            expected_profit = response - payload['Schluss'][0] 
            actual_value = df.iloc[first_index+days+1]['Schluss'].to_numpy()[0]
            actual_gain_or_loss = actual_value - payload['Schluss'][0]
            expected_profits[stock]= [expected_profit]
        
            determine_correct_gain_loss_predictions(stock, model)      
                     
            if stock in errors: 
                errors[stock][model] = (errors[stock][model] + abs(actual_value - response))
                
            else: # First error value, empty dict
                errors[stock] = [abs(actual_value - response)]
            
                 
               
print("\n-------------------------------- Statistics --------------------------------")
print("\nSimulation period: " + startdate + " + " + str(timeperiod) + " days")
for key in errors:
    for model_error in range(len(errors[key])):
        print("\n" + key + ", Model " + str(model_error+1))
        print("Mean Absolute Error (MAE): " + str(round((errors[key][model_error]/timeperiod),4))) 
        print("Percentage of correct Gain/Loss predictions: " + str(round(((gain_or_loss_correct_predictions[key][model]/timeperiod)*100), 2 )) + "%")
print("\n----------------------------------------------------------------------------")  
print("\n")
                     
# print("Buying stock " + max(expected_profits, key=expected_profits.get) + " with expected profit of " + str(max(expected_profits.values())) + "€" )
# print("Actual profit: " + actual_profit)           
    
