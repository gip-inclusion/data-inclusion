WITH source AS (
    SELECT * FROM {{ source('insee', 'departements') }}
),

final AS (
    SELECT
        "DEP"     AS "code",
        "LIBELLE" AS "libelle"
    FROM source
)

SELECT * FROM final
