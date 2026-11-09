# airflow and python versions come from the base image tag, kept up to date by dependabot
AIRFLOW_IMAGE := $(shell sed -nE 's|^FROM apache/airflow:([^@ ]+).*|\1|p' ../Dockerfile)
AIRFLOW_VERSION := $(firstword $(subst -python, ,$(AIRFLOW_IMAGE)))
PYTHON_VERSION := $(word 2,$(subst -python, ,$(AIRFLOW_IMAGE)))
PYPROJECT_PYTHON_VERSION := $(shell sed -nE 's|^requires-python = ">=([0-9.]+)"|\1|p' ../pyproject.toml)

ifeq ($(AIRFLOW_VERSION)$(PYTHON_VERSION),$(AIRFLOW_VERSION))
$(error could not read the airflow and python versions from ../Dockerfile)
endif
ifneq ($(PYPROJECT_PYTHON_VERSION),$(PYTHON_VERSION))
$(error requires-python >=$(PYPROJECT_PYTHON_VERSION) in ../pyproject.toml does not match python $(PYTHON_VERSION) in ../Dockerfile)
endif

CONSTRAINTS_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

UV_COMPILE := uv pip compile --quiet --python-version $(PYTHON_VERSION)

# `make upgrade all` recompiles with --upgrade
ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
UV_COMPILE += --upgrade
endif

PIP_COMPILE := $(UV_COMPILE) --constraint airflow/constraints.txt

# regenerated on every run: git does not preserve mtimes, so they cannot be trusted
# (stale-looking lock files after checkout, hand-edited requirements.txt).
# constraints.txt stays mtime-based to avoid fetching it on every run (CI refetches it).
.PHONY: all upgrade ../.python-version airflow/requirements.txt tasks/requirements.txt dev/requirements.txt dbt/requirements.txt

all: ../.python-version airflow/requirements.txt tasks/requirements.txt dev/requirements.txt dbt/requirements.txt

# generated for uv (venvs, `uv run`)
../.python-version:
	echo $(PYTHON_VERSION) > $@

# refetched when the python or airflow version changes
# upstream constraints do not pin airflow itself, hence the appended pin
airflow/constraints.txt: ../Dockerfile Makefile
	curl --fail --silent --show-error --location $(CONSTRAINTS_URL) --output $@.tmp
	printf '\n# from ../Dockerfile\napache-airflow==%s\n' $(AIRFLOW_VERSION) >> $@.tmp
	mv $@.tmp $@

airflow/requirements.txt: airflow/requirements.in airflow/constraints.txt
	$(PIP_COMPILE) $< --output-file=$@

tasks/requirements.txt: tasks/requirements.in airflow/constraints.txt
	$(PIP_COMPILE) $< --output-file=$@

dev/requirements.txt: dev/requirements.in airflow/constraints.txt
	$(PIP_COMPILE) $< --output-file=$@

# dbt runs in a separate environment (uvx), hence no airflow constraints
dbt/requirements.txt: dbt/requirements.in
	$(UV_COMPILE) $< --output-file=$@
