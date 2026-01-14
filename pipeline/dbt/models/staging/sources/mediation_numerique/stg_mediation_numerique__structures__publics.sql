WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

publics AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.publics_specifiquement_adresses[*]') AS item (data)
),

final AS (
    SELECT publics.*
    FROM publics
    INNER JOIN structures ON publics.structure_id = structures.id
    WHERE publics.value IS NOT NULL
)

SELECT * FROM final
