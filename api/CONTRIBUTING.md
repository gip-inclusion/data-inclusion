# Contributing

## Setup

```bash
# Create a new virtualenv in the project's root directory
python3 -m venv .venv --prompt di-api

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
uvicorn data_inclusion.api.app:app --reload
```

## Initialize the Database with Data from staging or prod

### Prerequisites:
1. Launch Docker Compose.
2. Set up MinIO alias.

Check the [Deployment Guide](../DEPLOYMENT.md) for more details.

```bash
# Copy staging (or production) data mart to your local MinIO instance
mc cp --recursive staging/data-inclusion-datalake-staging-sincere-buzzard/data/marts/2024-06-12/ dev/data-inclusion-lake/data/marts/2024-06-12

# Activate the virtual environment and install dependencies
source .venv/bin/activate

# Launch command to import the Admin Express database
python src/data_inclusion/api/cli.py import_admin_express

# Launch command to import data
python src/data_inclusion/api/cli.py load_inclusion_data
```

## Initialize the Database with data compute by airflow locally

You can also run locally airflow (with potentially less sources or only the sources that interest you).
After running the main dag:
```bash
# Activate the virtual environment and install dependencies
source .venv/bin/activate

# Launch command to import the Admin Express database
python src/data_inclusion/api/cli.py import_communes

# Launch command to import data
python src/data_inclusion/api/cli.py load_inclusion_data
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

make
```

### 2. Upgrading packages

```bash
make upgrade all
```

### Infrastructure

The app is deployed on Scalingo. Make sure you have access to the console.

Just like Scaleway, it can be useful to install the [CLI](https://doc.scalingo.com/platform/cli/start).

You also need to upload your [public key](https://www.scaleway.com/en/docs/dedibox-console/account/how-to/upload-an-ssh-key/) for SSH connection. You can use the same key as Scaleway.

Here are three useful commands (example for staging):

```bash
# Open psql
scalingo -a data-inclusion-api-staging pgsql-console

# Launch a one-off container
scalingo -a data-inclusion-api-staging run bash

# Open a tunnel
scalingo -a data-inclusion-api-staging db-tunnel SCALINGO_POSTGRESQL_URL
```

Once you launch the tunnel, you need a user to finish opening the connection. You can create one from the DB dashboard in the user tab.