FROM python:3.11-slim-bookworm

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app
RUN apt-get update && apt-get install -y git
RUN apt install -y python3-boto3
RUN pip install dagster dagster-webserver dagit boto3 pandas pyarrow botocore dagster-postgres SQLAlchemy psycopg2-binary
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
RUN pip install dvc
ADD .dvc .

RUN git config user.email "bajonettgaming@gmail.com"
RUN git config user.name "PriXss"

EXPOSE 3000

CMD [ "dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000" "-v" "$HOME/.ssh:/root/.ssh"]

