# installed by `make setup`, pinned by requirements/dbt/requirements.txt
DBT := $(CURDIR)/.venv/bin/dbt

# python version comes from the base image tag (see requirements/Makefile)
PYTHON_VERSION := $(shell sed -nE 's|^FROM apache/airflow:[^@ ]+-python([0-9.]+).*|\1|p' Dockerfile)

ifeq ($(PYTHON_VERSION),)
$(error could not read the python version from Dockerfile)
endif

# recreate the venv when its python does not match the image
define ensure_venv
	grep -qE '^version_info = $(PYTHON_VERSION)\.' $(1)/pyvenv.cfg 2>/dev/null \
		|| uv venv --clear --python $(PYTHON_VERSION) $(1)
endef

.PHONY: setup dbt-init

setup:
	$(call ensure_venv,.venv)
	uv pip sync requirements/dev/requirements.txt
	# dbt conflicts with the airflow constraints: install it in a dedicated venv
	# and expose its executable in the main venv
	$(call ensure_venv,.venv-dbt)
	uv pip sync --python .venv-dbt requirements/dbt/requirements.txt
	ln -sf ../../.venv-dbt/bin/dbt .venv/bin/dbt

dbt-init:
	cd dbt && $(DBT) deps
	cd dbt && $(DBT) seed
	cd dbt && $(DBT) run-operation create_udfs
