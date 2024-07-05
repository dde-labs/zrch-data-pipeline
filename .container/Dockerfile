FROM apache/airflow:2.9.0-python3.11

USER root

COPY packages.txt /

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        vim \
        wget \
    && xargs apt-get -y install $(grep -vE "^\s*#" /packages.txt | tr "\n" "\t") \
    && apt-get autoremove -yqq --purge \
    && apt-get clean

USER airflow

# This setting will fix airflow cannot import include/ to dags/
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"

COPY --chown=airflow:root airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY --chown=airflow:root requirements.txt /

RUN uv pip install  \
    --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt
