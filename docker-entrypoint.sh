#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

export DJANGO_SETTINGS_MODULE=settings."${ENV:-prod}"
export STATIC_ROOT=/var/www/static

if [[ "$#" -gt 0 ]]; then
  python django/manage.py "$@"
else
  if [[ "${ENV}" == "dev" ]]; then
    python django/manage.py migrate
    python django/manage.py collectstatic --no-input
    python django/manage.py createsuperuser --no-input || echo "Skipping."
    python django/manage.py runserver 0.0.0.0:8000
  else
    python django/manage.py check --deploy
    gunicorn -b 0.0.0.0:8000 --chdir django wsgi
  fi
fi