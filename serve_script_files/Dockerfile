FROM ludwigai/ludwig
ARG model_name
ENV model_name $model_name
WORKDIR /src
COPY ./ /src
EXPOSE 8000
RUN chmod +x serve_model.sh
ENTRYPOINT ["python3", "serve_model.py"]
