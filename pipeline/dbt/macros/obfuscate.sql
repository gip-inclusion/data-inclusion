{% macro obfuscate(field) %}
CASE WHEN {{ field }} IS NOT NULL THEN '***' ELSE NULL END
{% endmacro %}