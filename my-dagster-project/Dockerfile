FROM python:3.8-slim

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app


RUN pip install dagster dagster-webserver dagit dagster-postgres SQLAlchemy==1.4.49 pandas pyarrow
RUN pip install dvc

# Copy our code and workspace to /usr/src/app
COPY dagster.yaml .
COPY workspace.yaml .
COPY my_dagster_project ./my_dagster_project
COPY pyproject.toml .
COPY setup.cfg .
COPY setup.py .



EXPOSE 3000

CMD ["dvc", "init","dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

COPY ../.dvc/config ./.dvc