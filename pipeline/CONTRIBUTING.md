# Contributing

## Setup

```bash
make setup
make dbt-init  # requires running datawarehouse
```

## Running `dbt` against the datawarehouse

```bash
# run dbt commands in the dbt project dir
cd dbt

uv run dbt ls
uv run dbt build --select <model>
```

## Updating schema in dbt seeds

* Required when the schema changes.

```bash
./scripts/update_schema_seeds.py
```

## Managing requirements

In order to prevent conflicts:

* tasks requirements are compiled separately
* python tasks run in dedicated virtual envs using `task.virtualenv`
* python tasks requirements must comply to airflow constraints
* dbt tasks have their own separate requirements
* the airflow and python versions come from the base image tag in `Dockerfile` (bumped by dependabot): after a change, run `make all` (regenerates requirements and `.python-version`, CI fails when they are out of date) and `make -C .. setup` (recreates local venvs). `requires-python` in `pyproject.toml` must match, enforced by `make`


```bash
# cd to pipeline/requirements

# to simply add or remove a dependency
make all

# to upgrade dependencies
make upgrade all
```
