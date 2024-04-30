WITH source AS (
    SELECT * FROM {{ source('insee', 'regions') }}
),

final AS (
    SELECT
        "REG"     AS "code",
        "LIBELLE" AS "libelle"
    FROM source
)

SELECT * FROM final
