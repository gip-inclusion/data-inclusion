{% macro udf__slugify() %}

DROP FUNCTION IF EXISTS slugify;

CREATE OR REPLACE FUNCTION slugify(str TEXT)
RETURNS
    TEXT
AS $$

import slugify

return slugify.slugify(str)

$$ LANGUAGE plpython3u;

{% endmacro %}
