{% macro udf__format_phone_number() %}

DROP FUNCTION IF EXISTS processings.format_phone_number;

CREATE OR REPLACE FUNCTION processings.format_phone_number(phone TEXT)
    RETURNS TEXT
AS $$

from data_inclusion import processings

return processings.format_phone_number(phone)

$$ LANGUAGE plpython3u;

{% endmacro %}
