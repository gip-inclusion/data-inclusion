PIP_COMPILE := uv pip compile --quiet

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

AIRFLOW_VERSION := 2.10.1
PYTHON_VERSION := 3.12
CONSTRAINTS_FILE_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

.PHONY: all airflow dbt pipx python dev upgrade constraints

all: airflow dbt pipx python dev constraints

# https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files
constraints:
	wget --quiet $(CONSTRAINTS_FILE_URL) -O airflow/constraints.txt

# airflow requirements for scalingo deployments
# unlike docker based deployments, scalingo requires airflow to be installed from pypi
airflow: airflow/requirements.in constraints
	$(PIP_COMPILE) --constraint airflow/constraints.txt airflow/requirements.in --output-file=airflow/requirements.txt

# isolated env for dbt, which should be invoked using a bash operator
dbt: tasks/dbt/requirements.in
	$(PIP_COMPILE) tasks/dbt/requirements.in --output-file=tasks/dbt/requirements.txt

# isolated env for pipx, which should be invoked using a bash operator
pipx: tasks/pipx/requirements.in
	$(PIP_COMPILE) tasks/pipx/requirements.in --output-file=tasks/pipx/requirements.txt

# python based tasks requirements must be compatible with airflow requirements
# therefore these requirements are compiled with airflow constraints
python: airflow tasks/python/requirements.in
	$(PIP_COMPILE) --constraint airflow/constraints.txt tasks/python/requirements.in --output-file=tasks/python/requirements.txt

dev: python dev/requirements.in
	$(PIP_COMPILE) --constraint airflow/constraints.txt dev/requirements.in --output-file=dev/requirements.txt
