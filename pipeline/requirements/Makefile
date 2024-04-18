PIP_COMPILE := pipx run uv pip compile --quiet

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

AIRFLOW_VERSION := 2.9.0
PYTHON_VERSION := 3.11
UV_VERSION := 0.1.33
CONSTRAINTS_FILE_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

.PHONY: all airflow dbt pipx python dev upgrade constraints uv

all: uv airflow dbt pipx python dev constraints

uv:
	pip install uv==$(UV_VERSION)

# https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files
constraints:
	wget --quiet $(CONSTRAINTS_FILE_URL) -O airflow/constraints.txt

# airflow requirements for scalingo deployments
# unlike docker based deployments, scalingo requires airflow to be installed from pypi
airflow: airflow/requirements.in constraints uv
	$(PIP_COMPILE) --constraint airflow/constraints.txt airflow/requirements.in

# isolated env for dbt, which should be invoked using a bash operator
dbt: tasks/dbt/requirements.in uv
	$(PIP_COMPILE) tasks/dbt/requirements.in

# isolated env for pipx, which should be invoked using a bash operator
pipx: tasks/pipx/requirements.in uv
	$(PIP_COMPILE) tasks/pipx/requirements.in

# python based tasks requirements must be compatible with airflow requirements
# therefore these requirements are compiled with airflow constraints
python: airflow tasks/python/requirements.in uv
	$(PIP_COMPILE) --constraint airflow/constraints.txt tasks/python/requirements.in

dev: python dev/requirements.in uv
	$(PIP_COMPILE) --constraint airflow/constraints.txt dev/requirements.in
