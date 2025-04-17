{% macro udf__brevo_import_contacts() %}

DROP FUNCTION IF EXISTS processings.brevo_import_contacts;

CREATE OR REPLACE FUNCTION processings.brevo_import_contacts()
RETURNS
    TABLE(
        courriel TEXT,
        has_hardbounced BOOLEAN,
        was_objected_to BOOLEAN
    )
AS $$

import json

from data_inclusion import processings

return processings.brevo_import_contacts()
$$ LANGUAGE plpython3u;

{% endmacro %}
