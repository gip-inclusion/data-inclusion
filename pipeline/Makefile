# installed by `make setup`, pinned by requirements/dbt/requirements.txt
DBT := $(CURDIR)/.venv/bin/dbt

.PHONY: setup dbt-init

setup:
	uv venv --allow-existing
	uv pip sync requirements/dev/requirements.txt
	# dbt conflicts with the airflow constraints: install it in a dedicated venv
	# and expose its executable in the main venv
	uv venv --allow-existing .venv-dbt
	uv pip sync --python .venv-dbt requirements/dbt/requirements.txt
	ln -sf ../../.venv-dbt/bin/dbt .venv/bin/dbt

dbt-init:
	cd dbt && $(DBT) deps
	cd dbt && $(DBT) seed
	cd dbt && $(DBT) run-operation create_udfs
