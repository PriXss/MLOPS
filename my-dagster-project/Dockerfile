FROM python:3.11-slim-bookworm

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app/my-dagster-project
RUN apt-get update && apt-get install -y git

RUN apt install gcc musl-dev python3-dev -y

RUN git clone https://github.com/PriXss/MLOPS.git .
RUN git checkout DagsterPipelineProdRun
RUN git pull


WORKDIR /usr/src/app/my-dagster-project

RUN apt install -y python3-boto3
RUN pip install dagster dagster-webserver dagit boto3 pandas pyarrow botocore dagster-postgres SQLAlchemy psycopg2-binary

RUN pip install -r requirements.txt
#RUN pip install ludwig
COPY .env .

RUN pip install dvc

RUN git config --global user.email "bajonettgaming@gmail.com"
RUN git config --global user.name "PriXss"

EXPOSE 3000

CMD ["dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

