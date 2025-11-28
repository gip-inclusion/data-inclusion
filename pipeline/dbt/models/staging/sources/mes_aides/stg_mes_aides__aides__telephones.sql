{% set fields = ["Contact Tel", "Contact Tel 2", "Contact Tel 3"] %}

WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    {% for field in fields %}
        SELECT
            -- noqa: disable=layout.spacing
            data ->> 'ID'                            AS "aide_id",
            {{ loop.index }}                         AS "index",
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
