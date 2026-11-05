# Contributing

## Setup

```bash
uv sync
```

## Running the api locally

```bash
# Copy (and optionally edit) the template .env
cp .template.env .env

# Running the migrations
uv run alembic upgrade head

# Start the development server
uv run uvicorn data_inclusion.api.app:app --reload
```

### Generate token

```bash
uv run data-inclusion-api generate-token --help
```

### Load data

To load inclusion data, you can either use the local or the actual prod datalake (with RO creds).

```bash
uv run data-inclusion-api import-communes
uv run data-inclusion-api load-inclusion-data
```

## Running the test suite

```bash
uv run pytest
```

## Running the benchmark

```bash
uv run locust --headless --users 1 --run-time 120s -H http://localhost:8000
```
