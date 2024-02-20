FROM python:3.11.7-slim

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app

RUN pip install dagster dagster-webserver dagit boto3 pandas pyarrow

COPY dagster.yaml workspace.yaml .
COPY quickstart_etl ./quickstart_etl
COPY pyproject.toml .
COPY setup.cfg .
COPY setup.py .

EXPOSE 3000

CMD [ "dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

