WITH source AS (
    {{ stg_source_header('dora', 'services') }}),

services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                                         AS "service_id",
        NULLIF(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_orientation_accompagnateur')), '') AS "item"
    FROM source
)

SELECT final.*
FROM final
-- keep only values for selected services
INNER JOIN services ON final.service_id = services.id
