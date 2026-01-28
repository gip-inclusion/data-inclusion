WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

frais AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.frais_a_charge[*]') AS item (data)
),

final AS (
    SELECT frais.*
    FROM frais
    INNER JOIN structures ON frais.structure_id = structures.id
    WHERE frais.value IS NOT NULL
)

SELECT * FROM final
