#!/bin/bash

set -e

# perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

psql  --dbname="$POSTGRES_DB" <<- 'EOSQL'
CREATE SCHEMA IF NOT EXISTS processings;
EOSQL


psql  --dbname="$POSTGRES_DB" <<- 'EOSQL'
SET search_path TO processings;

DROP FUNCTION IF EXISTS geocode;
CREATE OR REPLACE FUNCTION geocode(
    data JSONB,
    batch_size INT DEFAULT 1000
)
RETURNS
    TABLE(
        id TEXT,
        result_score FLOAT,
        result_label TEXT,
        result_city TEXT,
        result_type TEXT,
        result_citycode TEXT,
        result_postcode TEXT
    )
AS $$

import json

from data_inclusion import processings

return (
    processings.geocode(data=json.loads(data), batch_size=batch_size)
    if data is not None
    else []
)

$$ LANGUAGE plpython3u;
EOSQL
