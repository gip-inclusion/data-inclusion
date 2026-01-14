WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

dispositifs AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.dispositif_programmes_nationaux[*]') AS item (data)
),

final AS (
    SELECT dispositifs.*
    FROM dispositifs
    INNER JOIN structures ON dispositifs.structure_id = structures.id
    WHERE dispositifs.value IS NOT NULL
)

SELECT * FROM final
