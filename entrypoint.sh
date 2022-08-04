#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

pip install -e .
gunicorn data_inclusion.api.entrypoints.fastapi:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile '-'
