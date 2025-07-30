#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

# The `DATABASE_URL` env var is automatically set by Scalingo.
# It uses the depreciated scheme `postgres://`.
# Fixing the env variable from the UI is not enough,
# because when upgrading the database, Scalingo overwrites it.
# The following line fixes the scheme at runtime.
export DATABASE_URL="${DATABASE_URL/postgres\:\/\//postgresql\:\/\/}"

gunicorn data_inclusion.api.app:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile '-' \
    --max-requests=1000 \
    --max-requests-jitter=100
