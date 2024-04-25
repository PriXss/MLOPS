FROM python:3.11.7

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app
RUN apt install -y git
RUN pip install hdf5
RUN pip install dagster dagster-webserver dagit boto3 pandas pyarrow botocore dagster-postgres SQLAlchemy
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install ludwig
COPY dagster.yaml .
COPY workspace.yaml .
COPY quickstart_etl ./quickstart_etl
COPY pyproject.toml .
COPY setup.cfg .
COPY setup.py .
COPY .env .
COPY .dvc .

EXPOSE 3000

CMD [ "dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

