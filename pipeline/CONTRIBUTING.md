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
pip-compile requirements/requirements.in > requirements/requirements.txt
pip-compile requirements/dev-requirements.in > requirements/dev-requirements.txt
```

## Running the containers

You must have followed the Setup steps above.

Depending on how you installed docker, you may use either `docker-compose` or `docker compose`.

Run all commands from the repository root directory (same level as `docker-compose.yml`)

You can check the status of the containers with 

```shell
docker compose ps
```

You can check the logs of a single container with

```shell
docker compose logs [airflow | minio | target-db | ... ]
```

### Database

```shell
docker compose up -d target-db
```

### Airflow

Build (if necessary) and start Airflow (Web service + task scheduler):

```shell
docker compose up -d airflow
```

Airflow Web service will be available at http://localhost:8080

### API

```shell
docker compose up -d api
```

The API front will be running at http://localhost:8000 (e.g. http://localhost:8000/api/v0/docs)

### minio

We use minio to mock our remote S3 for local development.

```shell
docker compose up -d minio
docker compose up minio-extra
```