WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_BASSIN_RESSOURCE') }}
),


final AS (
    SELECT
        "ID_BAS"          AS "id",
        "ID_RES"          AS "id_res",
        "CODE_BASSIN_BAS" AS "code_bassin_bas"

    FROM source
)

SELECT * FROM final
