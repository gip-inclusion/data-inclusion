WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_DEPARTEMENT_RESSOURCE') }}
),


final AS (
    SELECT
        "ID_DPT"                AS "id_dpt",
        "ID_RES"                AS "id_res",
        "CODE_DEPARTEMENT_DPT"  AS "code_departement_dpt",
        CONCAT('departement_ressource_', "ID_DPT") AS "id"

    FROM source
)

SELECT * FROM final
