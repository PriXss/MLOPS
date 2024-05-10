FROM glennverhaag/mlops:mlops-base-latest

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app/my-dagster-project

RUN pip install dvc_s3
RUN git clone https://PriXss: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@github.com/PriXss/MLOPS.git --branch=DagsterPipelineProdRun .
RUN git pull

RUN cd my-dagster-project

RUN git remote set-url origin  https://PriXss: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@github.com/PriXss/MLOPS.git
RUN git config --local user.email "PriXss@users.noreply.github.com"
RUN git config --local user.name "Marcel"

WORKDIR /usr/src/app/my-dagster-project

COPY .env .

EXPOSE 3000

CMD ["dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

