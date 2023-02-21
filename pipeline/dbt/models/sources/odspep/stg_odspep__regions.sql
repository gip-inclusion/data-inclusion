WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_1') }}
    UNION
    SELECT *
    FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_2') }}

),


final AS (
    SELECT
        "ID_REG"          AS "id",
        "ID_REG"          AS "id_reg",
        "ID_RES"          AS "id_res",
        "CODE_REGION_REG" AS "code_region_reg",
        'Région'          AS zone_diffusion_type

    FROM source
)

SELECT * FROM final
