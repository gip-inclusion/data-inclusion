PIP_COMPILE := uv pip compile --quiet --constraint airflow/constraints.txt

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

AIRFLOW_VERSION := 3.1.0
PYTHON_VERSION := 3.13

.PHONY: all airflow tasks dev upgrade

all: airflow tasks dev

airflow: airflow/requirements.in
	$(PIP_COMPILE) airflow/requirements.in --output-file=airflow/requirements.txt

tasks: airflow tasks/requirements.in
	$(PIP_COMPILE) tasks/requirements.in --output-file=tasks/requirements.txt

dev: tasks dev/requirements.in
	$(PIP_COMPILE) dev/requirements.in --output-file=dev/requirements.txt
