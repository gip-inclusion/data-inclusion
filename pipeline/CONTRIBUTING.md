# Contributing

## Setup

```bash
# Create a new virtualenv in the project's root directory
python3.10 -m venv .venv --prompt di-scripts

# Activate the environment
source .venv/bin/activate
pip install -U pip setuptools wheel

# Install dev dependencies
pip install -r requirements/dev-requirements.txt
```

## Running the test suite

```bash
# simply use tox (for reproducible environnement, packaging errors, etc.)
tox
```

## Updating the requirements

```bash
# optionally bump the airflow version
export AIRFLOW_VERSION=
export PYTHON_VERSION=3.10
curl https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt > requirements/constraints.txt
# edit and replace the `apache-airflow` package version
vim requirements/requirements.in

# compile the udpated requirements
pip-compile requirements/requirements.in > requirements/requirements.txt && \
    pip-compile requirements/dev-requirements.in > requirements/dev-requirements.txt
```

## dbt

* dbt in configure to target the `target-db` postgres container (see the root `docker-compose`).
* all dbt commands must be run in the in the `pipeline/dbt` directory.

```bash
# install pipx
pip install pipx

# alias dbt
alias dbt="pipx run --spec dbt-postgres dbt"

# install extra dbt packages (e.g. dbt_utils)
dbt deps

# run commands

#sources, basic processing/mapping:
#- retrieve data from datalake table
#- retrieve data from raw dedicated source tables
#- retrieve data from the Soliguide S3 
dbt run --select sources

# intermediate, specific transformations
dbt run --select intermediate

# marts, last touch
dbt run --select marts
```