{% macro udf__sync_emails() %}

DROP FUNCTION IF EXISTS processings.sync_emails;

CREATE OR REPLACE FUNCTION processings.sync_emails(data JSONB, all_contacts_list_id INT, current_contacts_list_id INT)
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
    processings.sync_emails(
        emails=json.loads(data),
        all_contacts_list_id=all_contacts_list_id,
        current_contacts_list_id=current_contacts_list_id,
    )
    if data is not None
    else []
)

$$ LANGUAGE plpython3u;

{% endmacro %}
