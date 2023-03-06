WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_1') }}
    UNION
    SELECT *
    FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_2') }}

),

insee_regions AS (
    SELECT * FROM {{ ref('insee_regions') }}
),

final AS (
    SELECT
        source."ID_REG"          AS "id",
        source."ID_REG"          AS "id_reg",
        source."ID_RES"          AS "id_res",
        source."CODE_REGION_REG" AS "code_region_reg",
        'Région'                 AS "zone_diffusion_type",
        insee_regions.label      AS "label"

    FROM source
    LEFT JOIN insee_regions ON source."CODE_REGION_REG" = insee_regions.code

)

SELECT * FROM final
