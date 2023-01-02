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

## Running airflow locally

```bash
# Copy the template .env
cp .template.env .env

# Edit the content of dotenv file to your needs

# Start the stack
docker-compose up -d
```

After a few seconds, the Airflow UI should be available at [http://localhost:8080](http://localhost:8080).

The default credentials are `airflow` / `airflow`.

The dags and `data_inclusion.scripts` package should be in editable mode : live modifications will be taken into account.

## Code quality

`pre-commit` hooks are provided and should be used !

## Contribution

Issues and PRs are welcome.

## Updating the requirements

```bash
# optionally bump the airflow version
export AIRFLOW_VERSION=
export PYTHON_VERSION=3.10
curl https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt > requirements/constraints.txt
# edit and replace the `apache-airflow` package version
vim requirements/requirements.in

# compile the udpated requirements
pip-compile requirements/requirements.in > requirements/requirements.txt
pip-compile requirements/dev-requirements.in > requirements/dev-requirements.txt
```