WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_DEPARTEMENT_RESSOURCE') }}
),


final AS (
    SELECT
        "ID_DPT"               AS "id",
        "ID_RES"               AS "id_res",
        "CODE_DEPARTEMENT_DPT" AS "code_departement_dpt"
    FROM source
)

SELECT * FROM final
