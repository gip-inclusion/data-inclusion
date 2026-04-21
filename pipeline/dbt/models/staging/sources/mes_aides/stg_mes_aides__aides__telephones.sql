{% set fields = ["tel", "tel1", "tel2", "tel3"] %}

WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    {% for field in fields %}
        SELECT
            -- noqa: disable=layout.spacing
            data ->> 'ID'                            AS "aide_id",
            GREATEST({{ loop.index }}, 2) - 1        AS "index",
            NULLIF(TRIM(data ->> '{{ field }}'), '') AS "value"
            -- noqa: enable=layout.spacing
        FROM source
        WHERE
            NULLIF(TRIM(data ->> '{{ field }}'), '') IS NOT NULL
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT * FROM final
