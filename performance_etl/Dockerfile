FROM apache/airflow:2.10.4-python3.11

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/airflow/performance_hub

WORKDIR /opt/airflow/performance_hub

USER root

COPY --chown=airflow:root pyproject.toml README.md requirements.txt ./
COPY --chown=airflow:root ingestion ./ingestion
COPY --chown=airflow:root script ./script
COPY --chown=airflow:root airflow ./airflow
COPY --chown=airflow:root sql ./sql
COPY --chown=airflow:root config ./config

RUN mkdir -p /opt/airflow/performance_hub/storage/manifests \
    /opt/airflow/performance_hub/storage/failed \
    /opt/airflow/performance_hub/storage/archive && \
    chown -R airflow:root /opt/airflow/performance_hub

USER airflow

RUN pip install .
