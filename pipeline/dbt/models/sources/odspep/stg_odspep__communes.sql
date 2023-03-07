WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_COMMUNE_RESSOURCE') }}
),

communes AS (
    SELECT * FROM {{ source('insee', 'communes') }}
),

final AS (
    SELECT
        source."ID_COM"           AS "id",
        source."ID_COM"           AS "id_com",
        source."ID_RES"           AS "id_res",
        source."CODE_COMMUNE_COM" AS "code_commune_com",
        communes."LIBELLE"        AS "libelle"

    FROM source
    LEFT JOIN communes ON source."CODE_COMMUNE_COM" = communes."COM"
)

SELECT * FROM final
