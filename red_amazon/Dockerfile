FROM glennverhaag/mlops:mlops-base-latest

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app/red_amazon

RUN pip install dvc_s3
RUN pip install alpaca-trade-api
RUN pip install ray
RUN pip install redis
RUN pip install python-dotenv

ARG TOKEN=invalidtoken

RUN git clone https://PriXss:${TOKEN}@github.com/PriXss/MLOPS.git --branch=red_amazon .

WORKDIR /usr/src/app/red_amazon

COPY .env.secrets /usr/src/app/red_amazon

EXPOSE 3000

CMD ["dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"                                                                                                                                                             ]
