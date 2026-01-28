WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

modalites_accompagnement AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.modalites_accompagnement[*]') AS item (data)
),

final AS (
    SELECT modalites_accompagnement.*
    FROM modalites_accompagnement
    INNER JOIN structures ON modalites_accompagnement.structure_id = structures.id
    WHERE modalites_accompagnement.value IS NOT NULL
)

SELECT * FROM final
