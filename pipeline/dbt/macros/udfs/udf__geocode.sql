{% macro udf__geocode() %}

DROP FUNCTION IF EXISTS processings.geocode;

CREATE OR REPLACE FUNCTION processings.geocode(data JSONB)
RETURNS
    TABLE(
        id TEXT,
        result_score FLOAT,
        result_label TEXT,
        result_city TEXT,
        result_type TEXT,
        result_citycode TEXT,
        result_postcode TEXT,
        result_name TEXT,
        longitude FLOAT,
        latitude FLOAT
    )
AS $$

import json

from data_inclusion import processings

return (
    processings.geocode(data=json.loads(data))
    if data is not None
    else []
)

$$ LANGUAGE plpython3u;

{% endmacro %}
