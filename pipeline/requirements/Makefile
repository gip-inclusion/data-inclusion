AIRFLOW_VERSION := 3.3.1
PYTHON_VERSION := 3.13

PIP_COMPILE := uv pip compile --quiet --constraint airflow/constraints.txt --python-version $(PYTHON_VERSION)

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

.PHONY: all airflow tasks dev dbt upgrade

all: airflow tasks dev dbt

airflow: airflow/requirements.in
	$(PIP_COMPILE) airflow/requirements.in --output-file=airflow/requirements.txt

tasks: airflow tasks/requirements.in
	$(PIP_COMPILE) tasks/requirements.in --output-file=tasks/requirements.txt

dev: tasks dev/requirements.in
	$(PIP_COMPILE) dev/requirements.in --output-file=dev/requirements.txt

# dbt runs in a separate environment (uvx), hence no airflow constraints so no PIP_COMPILE
dbt: dbt/requirements.in
	uv pip compile --quiet --python-version $(PYTHON_VERSION) $(if $(filter upgrade,$(MAKECMDGOALS)),--upgrade) dbt/requirements.in --output-file=dbt/requirements.txt
