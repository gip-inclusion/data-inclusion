# Contributing

## Setup

```bash
# Create a new virtualenv in the project's root directory
python3 -m venv .venv --prompt di-scripts

# Activate the environment
source .venv/bin/activate
pip install -U pip setuptools wheel

# Install the dev dependencies
pip install -r requirements/dev/requirements.txt
```

## Running the test suite

```bash
# Copy (and optionally edit) the template .env
cp .template.env .env

# simply use tox (for reproducible environnement, packaging errors, etc.)
tox
```

## dbt

* dbt is configured to target the `target-db` postgres container (see the root `docker-compose.yml`).
* all dbt commands must be run in the in the `pipeline/dbt` directory.

You can run dbt commands from your terminal.

```bash
# install extra dbt packages (e.g. dbt_utils)
dbt deps

# load seeds
dbt seed

# create user defined functions
dbt run-operation create_udfs

# run commands
dbt ls

# staging, basic processing/mapping:
# - retrieve data from datalake table
# - retrieve data from raw dedicated source tables
# - retrieve data from the Soliguide S3
dbt run --select staging

# intermediate, specific transformations
dbt run --select intermediate

# marts, last touch
dbt run --select marts
```

## Update schema in dbt seeds

* Required when the schema changes.

```bash
python scripts/update_schema_seeds.py
```

## Manage the pipeline requirements

In order to prevent conflicts:

* tasks requirements are compiled separately
* cli based tasks (like dbt and pipx) are also compiled separately
* python tasks run in dedicated virtuale envs using `ExternalPythonOperator`
* python tasks requirements must comply to airflow constraints


```bash
# cd to pipeline/requirements

# to simply add or remove a dependency
make all

# to upgrade dependencies
make upgrade all
```
