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
    && apt-get autoremove --purge -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements requirements

ENV VIRTUAL_ENV /venvs/python/venv
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"

RUN python -m venv ${VIRTUAL_ENV}
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements/tasks/python/requirements.txt

ENV VIRTUAL_ENV /venvs/dbt/venv
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"

RUN python -m venv ${VIRTUAL_ENV}
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements/tasks/dbt/requirements.txt

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

# set up the compiled virtual environments
ENV AIRFLOW_VAR_VENVS_DIR /opt/airflow/venvs
COPY --chown=airflow:airflow --from=compile-image /venvs /opt/airflow/venvs

# make the data_inclusion package available in editable mode
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/data-inclusion/src"
