WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_DEPARTEMENT_RESSOURCE') }}
),

insee_departements AS (
    SELECT * FROM {{ ref('insee_departements') }}
),

final AS (
    SELECT
        source."ID_DPT"               AS "id",
        source."ID_DPT"               AS "id_dpt",
        source."ID_RES"               AS "id_res",
        source."CODE_DEPARTEMENT_DPT" AS "code_departement_dpt",
        insee_departements.label

    FROM source
    LEFT JOIN insee_departements ON source."CODE_DEPARTEMENT_DPT" = insee_departements.code
)

SELECT * FROM final
