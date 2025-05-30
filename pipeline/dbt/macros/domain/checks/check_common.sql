{% macro create_udf__common_checks() %}
CREATE OR REPLACE FUNCTION CHECK_CODE_INSEE(value TEXT) RETURNS BOOLEAN AS $$
BEGIN
    RETURN value ~ '^.{5}$';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION CHECK_CODE_POSTAL(value TEXT) RETURNS BOOLEAN AS $$
BEGIN
    RETURN value ~ '^\d{5}$';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION CHECK_SIRET(value TEXT) RETURNS BOOLEAN AS $$
BEGIN
    RETURN value ~ '^\d{14}$';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION CHECK_RNA(value TEXT) RETURNS BOOLEAN AS $$
BEGIN
    RETURN value ~ '^W\d{9}$';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION CHECK_COURRIEL(value TEXT) RETURNS BOOLEAN AS $$
BEGIN
    -- RFC 5322
    RETURN value ~ '^[a-zA-Z0-9!#$%&''*+/=?^_`{|}~-]+[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION CHECK_ZONE_DIFFUSION_CODE(value TEXT) RETURNS BOOLEAN AS $$
BEGIN
    RETURN value ~ '^(\d{9}|\w{5}|\w{2,3}|\d{2})$';
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS processings.check_urls;
CREATE OR REPLACE FUNCTION processings.check_urls(data JSONB)
RETURNS
    TABLE(
        input_url TEXT,
        url TEXT,
        status_code INTEGER,
        error_message TEXT
    )
AS $$
import json
from data_inclusion import processings
return (
    processings.check_urls(data=json.loads(data))
    if data is not None
    else []
)
$$ LANGUAGE plpython3u;
{% endmacro %}
