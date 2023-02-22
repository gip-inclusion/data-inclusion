WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_DIR_TERRITORIALE_OFII') }}
),


final AS (
    SELECT
        "ID_DIT"                            AS "id_dit",
        "ID_RES"                            AS "id_res",
        "CODE_DIT"                          AS "code_dit",
        CONCAT('ofii_ressource_', "ID_DIT") AS "id"

    FROM source
)

SELECT * FROM final
