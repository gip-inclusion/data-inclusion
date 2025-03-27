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
# (Optional) Copy staging (or production) data mart to your local MinIO instance
mc cp --recursive \
    staging/data-inclusion-datalake-staging-sincere-buzzard/data/marts/2024-06-12/ \
    dev/data-inclusion-lake/data/marts/2024-06-12

# Load data
data-inclusion-api import_communes
data-inclusion-api load_inclusion_data
```

## Running the test suite

```bash
# simply use tox (for reproducible environnement, packaging errors, etc.)
tox
```

To modify communes.parquet:
```python
import json
import geopandas as gpd

def reverse_communes(dir: str):
    with open(f"{dir}/communes.json", "r", encoding="utf-8") as json_file:
        commune_data_list = json.load(json_file)

    df = gpd.GeoDataFrame.from_records(commune_data_list)

    df["centre"] = gpd.GeoSeries.from_wkt(df["centre"])
    df = df.set_geometry("centre")

    df.to_parquet(f"{dir}/communes.parquet")
```
Json file must be this format:
```json
[
    {
        "code": 59183,
        "nom": "Dunkerque",
        "departement": 59,
        "region": 32,
        "siren_epci": 245900428,
        "codes_postaux": [
            "59140",
            "59240",
            "59279"
        ],
        "centre": "POINT (2.3431 51.0169)"
    },
...
]
```


## Running the benchmark

```bash
locust --headless -u 3 -t 60s -H http://127.0.0.1:8001
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

### 3. Using a development version of data-inclusion-schema

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

