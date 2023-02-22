WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_COMMUNE_RESSOURCE') }}
),


final AS (
    SELECT
        "ID_COM"           AS "id",
        "ID_RES"           AS "id_res",
        "CODE_COMMUNE_COM" AS "code_commune_com"

    FROM source
)

SELECT * FROM final
