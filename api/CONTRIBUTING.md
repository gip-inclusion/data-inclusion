# Contributing

## Setup

```bash
# Create a new virtualenv in the project's root directory
python3.10 -m venv .venv --prompt di-api

# Activate the environment and install the dependencies
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -r requirements/dev-requirements.txt

# install the api application in editable mode
pip install -e .
```

## Running the api locally

Make sure the `target-db` container is up and running. See instructions [here](../CONTRIBUTING.md#docker).

```bash
# Copy (and optionally edit) the template .env
cp .template.env .env

# Running the migrations
alembic upgrade head

# Start the development server
uvicorn data_inclusion.api.entrypoints.fastapi:app --reload
```

## Running the test suite

```bash
# simply use tox (for reproducible environnement, packaging errors, etc.)
tox
```

## Updating the requirements

```bash
# 1. edit the requirements in setup.py

# 2. compile dependencies
pip-compile --output-file=requirements/requirements.txt
pip-compile --extra=dev --output-file=requirements/dev-requirements.txt
pip-compile --extra=test --output-file=requirements/test-requirements.txt
```