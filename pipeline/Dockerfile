########
# This image compiles virtual environments for airflow tasks
########
FROM python:3.12-slim AS compile-image

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

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
ENV VIRTUAL_ENV=/opt/airflow/venvs/python/venv

RUN python -m venv ${VIRTUAL_ENV}
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir -r requirements/tasks/python/requirements.txt

# virtual env path must match their location on the runtime image to be portable
ENV VIRTUAL_ENV=/opt/airflow/venvs/pipx/venv

RUN python -m venv ${VIRTUAL_ENV}
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir -r requirements/tasks/pipx/requirements.txt

# virtual env path must match their location on the runtime image to be portable
ENV VIRTUAL_ENV=/opt/airflow/venvs/dbt/venv

RUN python -m venv ${VIRTUAL_ENV}
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN "${VIRTUAL_ENV}/bin/python" -m pip install --no-cache-dir -r requirements/tasks/dbt/requirements.txt

########
# This image is the runtime
########
FROM apache/airflow:2.10.5-python3.12 AS runtime-image

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

ENV AIRFLOW_VAR_DBT_PROJECT_DIR=/opt/airflow/dbt

COPY requirements requirements
RUN pip install -r requirements/airflow/requirements.txt

# copy the compiled virtual environments to a `venvs` directory in the airflow home
COPY --chown=airflow:0 --from=compile-image /opt/airflow/venvs /opt/airflow/venvs

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

RUN /opt/airflow/venvs/python/venv/bin/playwright install-deps chromium

USER airflow

COPY --chown=airflow:0 pyproject.toml pyproject.toml
COPY --chown=airflow:0 dags /opt/airflow/dags
COPY --chown=airflow:0 dbt ${AIRFLOW_VAR_DBT_PROJECT_DIR}
COPY --chown=airflow:0 tests /opt/airflow/tests

RUN /opt/airflow/venvs/dbt/venv/bin/dbt deps --project-dir /opt/airflow/dbt
RUN /opt/airflow/venvs/python/venv/bin/playwright install chromium

# make the dags package available in editable mode
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags"
