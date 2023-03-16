FROM apache/airflow:2.5.1-python3.10

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

COPY requirements requirements

# disable `--user` flag for the next commands
# cf https://airflow.apache.org/docs/docker-stack/build.html#important-notes-for-the-base-images
ENV PIP_USER false

ENV VENVS_DIR /opt/airflow/venvs

# create a isolated virtualenvs for python task execution
RUN python -m venv "${VENVS_DIR}/python/venv"
RUN venvs/python/venv/bin/python -m pip install --no-cache-dir -U pip setuptools wheel
RUN venvs/python/venv/bin/python -m pip install --no-cache-dir -r requirements/tasks/python/requirements.txt

# create a isolated virtualenvs for python task execution
RUN python -m venv "${VENVS_DIR}/dbt/venv"
RUN venvs/dbt/venv/bin/python -m pip install --no-cache-dir -U pip setuptools wheel
RUN venvs/dbt/venv/bin/python -m pip install --no-cache-dir -r requirements/tasks/dbt/requirements.txt

ENV PIP_USER true

# make the data_inclusion package available in editable mode
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/data-inclusion/src"
