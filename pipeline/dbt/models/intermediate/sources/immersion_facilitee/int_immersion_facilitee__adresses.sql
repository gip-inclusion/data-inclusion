WITH structures AS (
    SELECT * FROM {{ ref('stg_immersion_facilitee__structures') }}
),

final AS (
    SELECT
        id                        AS "id",
        'immersion-facilitee'     AS "source",
        NULL                      AS "complement_adresse",
        city                      AS "commune",
        street_number_and_address AS "adresse",
        post_code                 AS "code_postal",
        NULL                      AS "code_insee",
        CAST(NULL AS FLOAT)       AS "longitude",
        CAST(NULL AS FLOAT)       AS "latitude"
    FROM structures
)

SELECT * FROM final
