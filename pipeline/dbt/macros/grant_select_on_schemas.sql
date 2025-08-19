{% macro grant_select_on_schemas_to_public(schemas) %}
{% for schema in schemas %}
GRANT USAGE ON SCHEMA {{ schema }} TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ schema }} GRANT SELECT ON TABLES TO PUBLIC;
{% endfor %}
{% endmacro %}
