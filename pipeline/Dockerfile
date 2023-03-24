########
# This image compiles virtual environments for airflow tasks
########
FROM python:3.10-slim AS compile-image

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements requirements

# virtual env path must match their location on the runtime image to be portable
ENV VIRTUAL_ENV /opt/airflow/venvs/python/venv

RUN python -m venv ${VIRTUAL_ENV}
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir -r requirements/tasks/python/requirements.txt

# virtual env path must match their location on the runtime image to be portable
ENV VIRTUAL_ENV /opt/airflow/venvs/dbt/venv

RUN python -m venv ${VIRTUAL_ENV}
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir -r requirements/tasks/dbt/requirements.txt

########
# This image is the runtime
########
FROM apache/airflow:2.5.1-python3.10

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# copy the compiled virtual environments to a `venvs` directory in the airflow home
COPY --chown=airflow:airflow --from=compile-image /opt/airflow/venvs /opt/airflow/venvs

# make the data_inclusion package available in editable mode
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/data-inclusion/src"
