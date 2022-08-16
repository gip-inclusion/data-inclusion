# Contributing

## Local prerequisites

### `vscode` (Recommended)

Support for vscode is provided, with a sensible configuration.

### `docker`

A `docker-compose.yml` file is provided for development.

### `python`

Project uses `python3.10`

[`pyenv`](https://github.com/pyenv/pyenv) is a clean and easy way to manage multiple python versions on your computer. Installation instructions are available [here](https://github.com/pyenv/pyenv-installer).

## Setup

```bash
# Clone this repository
git clone git@github.com:betagouv/data-inclusion.git

# Create a new virtualenv in the project's root directory
python3.10 -m venv .venv --prompt di-api

# Activate the environment and install the dependencies
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -r dev-requirements.txt

# install the api application in editable mode
pip install -e .

# Setup code quality tools
pre-commit install
```

## Running the api locally

```bash
# Copy (and optionally edit) the template .env
cp .template.env .env

# Launch the database
docker-compose up -d db

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

## Code quality

`pre-commit` hooks are provided and should be used !

## Contribution

Issues and PRs are welcome.
