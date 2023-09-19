# Contributing

## Setup

```bash
# Create a new virtualenv in the project's root directory
python3.10 -m venv .venv --prompt di-scripts

# Activate the environment
source .venv/bin/activate
pip install -U pip setuptools wheel

# Install the dev dependencies
pip install -r requirements/dev/requirements.txt
```

## Running the test suite

```bash
# simply use tox (for reproducible environnement, packaging errors, etc.)
tox
```

## dbt

* dbt is configured to target the `target-db` postgres container (see the root `docker-compose.yml`).
* all dbt commands must be run in the in the `pipeline/dbt` directory.

You can run dbt commands from your terminal.

```bash
# install dbt
pipx install --include-deps dbt-postgres==1.6.1

# install extra dbt packages (e.g. dbt_utils)
dbt deps

# load seeds
dbt seeds

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

## Project requirements

* `pip-compile~=7.3`

### airflow

These requirements are mainly used for the deployment on scalingo.

Unlike the local dev setup, which uses the airflow official docker image, scalingo
installs our requirements listed in our files.

When installing from PyPi, it is recommended to use the `constraints.txt` file ([source](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files)).

To update the constraints and upgrade the requirements:

```bash
# optionally bump the airflow version
AIRFLOW_VERSION=
PYTHON_VERSION=3.10
curl https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt > requirements/airflow/constraints.txt
pip-compile --upgrade requirements/airflow/requirements.in
```

### tasks

These sets of requirements are used to create isolated environments for python tasks on airflow.
They are independent from the airflow requirements.

- `requirements/tasks/dbt/`: requirements for dbt tasks
- `requirements/tasks/python/`: requirements for various python tasks (pandas, openpyxl, requests, etc.)

To add or delete a dependency to these requirements:

```bash
# 1. edit the target requirements/tasks/...../requirements.in
# 2. compile the dependencies
pip-compile requirements/tasks/dbt/requirements.in
pip-compile requirements/tasks/python/requirements.in
```

To upgrade these requirements:

```bash
pip-compile --upgrade requirements/tasks/dbt/requirements.in
pip-compile --upgrade requirements/tasks/python/requirements.in
```

Then you should update the dev requirements.

### dev

These requirements are used for local development (IDE completion, formatting, tests, etc.).
They merge the previous requirements, without constraints, and add dev packages (black, pytest, etc.).

To add or delete a dependency to these dev requirements:

```bash
# 1. edit the target requirements/dev/requirements.in
# 2. compile the dependencies
pip-compile requirements/dev/requirements.in
```

To upgrade these requirements:

```bash
pip-compile --upgrade requirements/dev/requirements.in
```