# Contributing

## Prerequisites

* `uv` : [installation instructions](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer).

## Installation

```bash
# Clone the repository
git clone git@github.com:gip-inclusion/data-inclusion.git

# Init environment
uv sync

# Install precommits
uv run pre-commit install
```

## Running the api locally

Make sure the `target-db` container is up and running. See instructions [here](../CONTRIBUTING.md#docker).

```bash
# Copy (and optionally edit) the template .env
cp .template.env .env

# Running the migrations
uv run alembic upgrade head

# Start the development server
uv run uvicorn data_inclusion.api.app:app --reload
```

## Initialize the Database with Data from staging or prod

### Prerequisites:

1. Launch Docker Compose.
2. Set up MinIO alias.

Check the [Deployment Guide](../DEPLOYMENT.md) for more details.

```bash
# (Optional) Copy staging (or production) data mart to your local MinIO instance
mc cp --recursive \
    staging/data-inclusion-datalake-staging-sincere-buzzard/data/marts/2024-06-12/ \
    dev/data-inclusion-lake/data/marts/2024-06-12

# Load data
uv run data-inclusion-api import_communes
uv run data-inclusion-api load_inclusion_data
```

## Running the test suite

```bash
# simply use tox (for reproducible environnement, packaging errors, etc.)
uv run tox
```

## Running the benchmark

```bash
uv run locust --headless -u 3 -t 60s -H http://127.0.0.1:8001
```

## Managing the app requirements

Checkout [uv's documentation](https://docs.astral.sh/uv/concepts/projects/dependencies/)

### Using a development version of data-inclusion-schema

In your [data-inclusion-schema](https://github.com/gip-inclusion/data-inclusion-schema) branch or pull request,
edit the `pyproject.toml` to reference a version number that is unheard of. For instance:

```
[project]
name = "data-inclusion-schema"
version = "0.21.0-<COMMIT_HASH>"
```

The reason being that upon schema installation, Scalingo will fetch the python package as a source, directly
from Github and check if it already knows the version specified in `pyproject.toml` in one of its caches.

Specifying a new version number there ensures that you will install that source package from Github and not
a pre-cached wheel.


Then, edit data⋅inclusion API's `setup.py` to target your branch, tag or specific commit:

```
data-inclusion-schema @ git+https://github.com/gip-inclusion/data-inclusion-schema.git@BRANCH_NAME
data-inclusion-schema @ git+https://github.com/gip-inclusion/data-inclusion-schema.git@TAG_NAME
data-inclusion-schema @ git+https://github.com/gip-inclusion/data-inclusion-schema.git@COMMIT_HASH
```

Then run `make all` to update all the requirement files; those changes can now be commited on your branch
to attempt a deployment on the staging environment, for instance.

Use `pip install --force-reinstall -r requirements/requirements.txt` to install the development version
of the schema locally (note the `--force-reinstall`)



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

