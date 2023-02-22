WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_COMMUNE_RESSOURCE') }}
),

insee_communes AS (
    SELECT * FROM {{ ref('insee_communes') }}
),

final AS (
    SELECT
        source."ID_COM"           AS "id",
        source."ID_COM"           AS "id_com",
        source."ID_RES"           AS "id_res",
        source."CODE_COMMUNE_COM" AS "code_commune_com",
        insee_communes.label

    FROM source
    LEFT JOIN insee_communes ON source."CODE_COMMUNE_COM" = insee_communes.code
)

SELECT * FROM final
