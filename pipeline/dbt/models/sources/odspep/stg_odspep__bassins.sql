WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_BASSIN_RESSOURCE') }}
),


final AS (
    SELECT
        "ID_BAS"                 AS "id_bas",
        "ID_RES"                 AS "id_res",
        "CODE_BASSIN_BAS"        AS "code_bassin_bas",
        CONCAT('bassin_ressource_', "ID_BAS") AS "id"

    FROM source
)

SELECT * FROM final
