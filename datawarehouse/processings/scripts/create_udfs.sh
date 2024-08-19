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


psql  --dbname="$POSTGRES_DB" <<- 'EOSQL'
DROP FUNCTION IF EXISTS validate;
DROP TYPE IF EXISTS pydantic_error;
DROP TYPE IF EXISTS resource_type;

CREATE TYPE resource_type AS ENUM ('structure', 'service');

CREATE TYPE pydantic_error AS (
    type  TEXT,
    loc   TEXT[],
    msg   TEXT,
    input TEXT
);

CREATE OR REPLACE FUNCTION validate(resource_type resource_type, data JSONB)
RETURNS SETOF pydantic_error AS $$

import json

import pydantic

from data_inclusion import schema

model = schema.Structure if resource_type == "structure" else schema.Service

try:
    model.model_validate_json(data)
except pydantic.ValidationError as e:
    return e.errors()

return []

$$ LANGUAGE plpython3u;
EOSQL
