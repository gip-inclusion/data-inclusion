#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

# The `DATABASE_URL` env var is automatically set by Scalingo and uses the depreciated
# scheme `postgres://`. Replace it.
export DATABASE_URL="${DATABASE_URL/postgres\:\/\//postgresql\:\/\/}"

pip install -e .

# Running database migrations
alembic upgrade head

gunicorn data_inclusion.api.entrypoints.fastapi:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile '-'
