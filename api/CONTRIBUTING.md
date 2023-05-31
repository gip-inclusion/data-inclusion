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

## Managing the app requirements

### 1. Adding/removing packages

```bash
# 1. add/remove packages from the requirements in setup.py

# 2. compile dependencies
pip-compile --resolver=backtracking --output-file=requirements/requirements.txt
pip-compile --resolver=backtracking --extra=dev --output-file=requirements/dev-requirements.txt
pip-compile --resolver=backtracking --extra=test --output-file=requirements/test-requirements.txt
```

### 2. Upgrading packages

```bash
# 1. compile dependencies with the upgrade flag set
pip-compile --resolver=backtracking --upgrade --output-file=requirements/requirements.txt && \
    pip-compile --resolver=backtracking --upgrade --extra=dev --output-file=requirements/dev-requirements.txt && \
    pip-compile --resolver=backtracking --upgrade --extra=test --output-file=requirements/test-requirements.txt
```