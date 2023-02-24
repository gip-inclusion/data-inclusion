WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_DIR_TERRITORIALE_OFII') }}
),


final AS (
    SELECT
        "ID_DIT"                    AS "id",
        "ID_RES"                    AS "id_res",
        "CODE_DIT"                  AS "code_dit"

    FROM source
)

SELECT * FROM final
