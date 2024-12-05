{% macro udf__deduplicate() %}

DROP FUNCTION IF EXISTS processings.deduplicate;

CREATE OR REPLACE FUNCTION processings.deduplicate(data JSONB)
RETURNS
    TABLE(
        cluster_id TEXT,
        score FLOAT,
        structure_id TEXT
    )
AS $$

import json

from data_inclusion import processings

return (
    processings.deduplicate(data=json.loads(data))
    if data is not None
    else []
)

$$ LANGUAGE plpython3u;

{% endmacro %}
