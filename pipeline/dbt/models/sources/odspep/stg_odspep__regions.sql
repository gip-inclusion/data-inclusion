WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_1') }}
    UNION
    SELECT *
    FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_2') }}

),

regions AS (
    SELECT * FROM {{ source('insee', 'regions') }}
),

final AS (
    SELECT
        source."ID_REG"          AS "id",
        source."ID_REG"          AS "id_reg",
        source."ID_RES"          AS "id_res",
        source."CODE_REGION_REG" AS "code_region_reg",
        'Région'                AS "zone_diffusion_type",
        regions."LIBELLE"        AS "libelle"

    FROM source
    LEFT JOIN regions ON source."CODE_REGION_REG" = regions."REG"

)

SELECT * FROM final
