{% macro unnest(from, column, foreign_key, fk_alias) %}
WITH source AS (
    SELECT
        {{ foreign_key }}    AS {{ fk_alias }},
        UNNEST({{ column }}) AS "value"
    FROM {{ from }}
)
SELECT
    {{ fk_alias }},
    value
FROM source
WHERE value IS NOT NULL
{% endmacro %}
