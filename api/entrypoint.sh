#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

gunicorn data_inclusion.api.app:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile '-'
