#!/bin/bash

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

"${psql[@]}"  --dbname="$POSTGRES_DB" <<- 'EOSQL'
CREATE EXTENSION IF NOT EXISTS pg_trgm;
EOSQL
