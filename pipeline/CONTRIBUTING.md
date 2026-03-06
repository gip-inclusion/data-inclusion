# Contributing

## Setup

```bash
uv venv --python 3.12
uv pip install -r requirements.txt
uv pip install dbt-postgres dbt-core==1.*
```

## First airflow start

Ensure required env vars are set (e.g. copy `.env.template` to `.env` and load those env vars in your shell)

Then from the `pipeline` folder:

```
./airflow.sh db migrate
./airflow.sh fab-db migrate
./airflow.sh users create --username admin --password admin --firstname Admin --lastname Admin --role Admin --email admin@example.com
```

Use `./airflow.sh` as the entry point for any airflow command (standalone, dags test, etc.).


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

## Testing the UDFs

From the `pipeline` folder:

```bash
uv venv
uv pip install -r ../datawarehouse/requirements.txt &&
uv run pytest dbt/macros/udfs/ -vv -m 'not integration'
```
