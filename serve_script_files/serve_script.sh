pip install boto3
python3 download_boto3_new_key.py
imagename=$(echo "$MODEL_NAME" | tr '[:upper:]' '[:lower:]')
echo "$imagename"
sudo docker build --build-arg model_name=${imagename} -t ${imagename} .
sudo docker run -d -it -p ${port}:8000 ${imagename}
