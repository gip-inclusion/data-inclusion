# Contributing

## Setup

```bash
uv venv
uv pip install -r requirements/dev/requirements.txt
uv pip install dbt-postgres dbt-core==1.*
```

## Running `dbt`

* dbt is configured to target the `datawarehouse` postgres container (see the root `docker-compose.yml`).

```bash
# run dbt commands in the dbt project dir
cd ./dbt

# install extra dbt packages (e.g. dbt_utils)
uv dbt deps
# load seeds
uv dbt seed
# create user defined functions
uv dbt run-operation create_udfs
# run commands
uv dbt ls
```

## Updating schema in dbt seeds

* Required when the schema changes.

```bash
./scripts/update_schema_seeds.py
```

## Managing the pipeline requirements

In order to prevent conflicts:

* tasks requirements are compiled separately
* python tasks run in dedicated virtual envs using `task.virtualenv`
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
