FROM apache/airflow:3.4.0-python3.13@sha256:a8d9d19c563c768b2bc605ef26440f28a9164ea886064e99c599b057432ff572 AS runtime-image

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements requirements
RUN uv --no-cache pip install -r requirements/airflow/requirements.txt

COPY --chown=airflow:0 pyproject.toml pyproject.toml
COPY --chown=airflow:0 dags dags
COPY --chown=airflow:0 dbt dbt
COPY --chown=airflow:0 tests tests

# make the dags package available in editable mode
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags"
