{% macro udf__sync_emails() %}

DROP FUNCTION IF EXISTS processings.sync_emails;

CREATE OR REPLACE FUNCTION processings.sync_emails(data JSONB)
RETURNS
    TABLE(
        courriel TEXT,
        has_hardbounced BOOLEAN,
        was_objected_to BOOLEAN
    )
AS $$

import json

from data_inclusion import processings

return (
    processings.sync_emails(emails=json.loads(data))
    if data is not None
    else []
)

$$ LANGUAGE plpython3u;

{% endmacro %}
