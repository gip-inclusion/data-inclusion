#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

# Running database migrations
alembic upgrade head

gunicorn data_inclusion.api.entrypoints.fastapi:app \
    --bind 0.0.0.0 \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile '-'
