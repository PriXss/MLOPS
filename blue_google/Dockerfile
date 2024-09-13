FROM glennverhaag/mlops:mlops-base-latest

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app/blue_google

RUN pip install dvc_s3
RUN pip install alpaca-trade-api
RUN pip install ray
RUN pip install redis
RUN pip install python-dotenv

ARG TOKEN=invalidtoken

RUN git clone https://PriXss:${TOKEN}@github.com/PriXss/MLOPS.git --branch=blue_google .

COPY ~/admin/.env.secrets /usr/src/app/blue_google

WORKDIR /usr/src/app/blue_google

EXPOSE 3000

CMD ["dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]