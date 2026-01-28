WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

modalites_acces AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.modalites_acces[*]') AS item (data)
),

final AS (
    SELECT modalites_acces.*
    FROM modalites_acces
    INNER JOIN structures ON modalites_acces.structure_id = structures.id
    WHERE modalites_acces.value IS NOT NULL
)

SELECT * FROM final
