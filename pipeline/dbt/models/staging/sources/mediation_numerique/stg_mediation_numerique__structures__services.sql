WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

services AS (
    SELECT
        source.data ->> 'id'                 AS "structure_id",
        NULLIF(TRIM(item.data #>> '{}'), '') AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.services[*]') AS item (data)
),

final AS (
    SELECT services.*
    FROM services
    INNER JOIN structures ON services.structure_id = structures.id
    WHERE services.value IS NOT NULL
)

SELECT * FROM final
