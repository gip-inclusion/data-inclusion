{% macro stg_source_header(src_name, table_name) %}

{% set source_model = source(src_name, table_name) %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}
    SELECT * FROM {{ source_model }}
{% else %}
    SELECT
        NULL                AS "_di_source_id",
        CAST(NULL AS JSONB) AS "data"
    WHERE FALSE
{% endif %}

{% endmacro %}
