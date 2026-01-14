WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

prises_en_charge AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.prise_en_charge_specifique[*]') AS item (data)
),

final AS (
    SELECT prises_en_charge.*
    FROM prises_en_charge
    INNER JOIN structures ON prises_en_charge.structure_id = structures.id
    WHERE prises_en_charge.value IS NOT NULL
)

SELECT * FROM final
