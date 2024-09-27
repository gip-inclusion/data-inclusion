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

# Install dbt
pip install -r requirements/tasks/dbt/requirements.txt
```

## Running `dbt`

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

dbt build --select models/staging
dbt build --select models/intermediate
dbt build --select models/marts
```

## Updating schema in dbt seeds

* Required when the schema changes.

```bash
python scripts/update_schema_seeds.py
```

## Managing the pipeline requirements

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

## Running the test suite

```bash
# Copy (and optionally edit) the template .env
cp .template.env .env

# simply use tox (for reproducible environnement, packaging errors, etc.)
tox
```
