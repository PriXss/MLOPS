pip install boto3
python3 download_boto3_new_key.py
sudo docker build --build-arg model_name=deep_lstm -t deep_lstm_image .
sudo docker run -it -p 8001:8000 deep_lstm_image
