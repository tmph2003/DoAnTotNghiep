# Sử dụng
FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y git libpq-dev gcc

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1


USER airflow
RUN --mount=type=bind,target=./requirements.txt,src=./requirements.txt \
    pip install -r requirements.txt

COPY --chown=50000 ../dags /app/dags
COPY --chown=50000 ../include /app/include
COPY --chown=50000 ../plugins /app/plugins

WORKDIR /app

