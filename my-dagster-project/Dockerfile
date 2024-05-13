FROM glennverhaag/mlops:mlops-base-latest

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app/my-dagster-project

RUN pip install dvc_s3 ludwig

ARG TOKEN=invalidtoken

RUN git clone https://PriXss:${TOKEN}@github.com/PriXss/MLOPS.git --branch=DagsterPipelineProdRun .

WORKDIR /usr/src/app/my-dagster-project

COPY .env .

EXPOSE 3000

CMD ["dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

