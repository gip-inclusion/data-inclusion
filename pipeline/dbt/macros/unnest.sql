{% macro unnest(from, column, foreign_key, fk_alias) %}

SELECT
    {{ foreign_key }}    AS {{ fk_alias }},
    UNNEST({{ column }}) AS "value"
FROM {{ from }}

{% endmacro %}
