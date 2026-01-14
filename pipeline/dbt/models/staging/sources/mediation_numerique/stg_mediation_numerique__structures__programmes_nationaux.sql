WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

programmes_nationaux AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.dispositif_programmes_nationaux[*]') AS item (data)
),

final AS (
    SELECT programmes_nationaux.*
    FROM programmes_nationaux
    INNER JOIN structures ON programmes_nationaux.structure_id = structures.id
    WHERE programmes_nationaux.value IS NOT NULL
)

SELECT * FROM final
