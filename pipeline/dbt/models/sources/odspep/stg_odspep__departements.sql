WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_DEPARTEMENT_RESSOURCE') }}
),

departements AS (
    SELECT * FROM {{ source('insee', 'departements') }}
),

final AS (
    SELECT
        source."ID_DPT"               AS "id",
        source."ID_DPT"               AS "id_dpt",
        source."ID_RES"               AS "id_res",
        source."CODE_DEPARTEMENT_DPT" AS "code_departement_dpt",
        departements."LIBELLE"        AS "libelle"

    FROM source
    LEFT JOIN departements ON source."CODE_DEPARTEMENT_DPT" = departements."DEP"
)

SELECT * FROM final
