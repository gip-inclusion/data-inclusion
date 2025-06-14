{% macro udf__check_urls() %}

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
