FROM glennverhaag/mlops:mlops-base-latest

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app/my-dagster-project

RUN mkdir -p -m 0600 ~/.ssh && \
    ssh-keyscan -H github.com bitbucket.org >> ~/.ssh/known_hosts
RUN --mount=type=ssh \
    git clone git@github.com:PriXss/MLOPS.git .

RUN git checkout DagsterPipelineProdRun
RUN git pull

RUN touch test.txt
RUN git add .
RUN git commit -m "Jo"
RUN git push --force

WORKDIR /usr/src/app/my-dagster-project

COPY .env .


EXPOSE 3000

CMD ["dagster-webserver" , "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

