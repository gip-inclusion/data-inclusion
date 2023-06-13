{% macro truncate_text(text) %}
CASE WHEN LENGTH({{ text }}) <= 280 THEN {{ text }} ELSE LEFT({{ text }}, 279) || '…' END
{% endmacro %}