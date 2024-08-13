{% macro unnest_jsonb_list_to_table(resource_type, nested_column) %}

WITH {{ resource_type }}s AS (
    SELECT * FROM {{ ref('marts_inclusion__{}s'.format(resource_type) ) }}
),

final AS (
    SELECT
        _di_surrogate_id AS "{{ resource_type }}_surrogate_id",
        value_           AS "value"
    FROM
        {{ resource_type }}s,
        UNNEST({{ nested_column }}) AS value_
)

SELECT * FROM final

{% endmacro %}
