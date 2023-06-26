WITH structures AS (
    SELECT * FROM {{ ref('stg_immersion_facilitee__structures') }}
),

final AS (
    SELECT
        id                        AS "id",
        NULL                      AS "longitude",
        NULL                      AS "latitude",
        'immersion-facilitee'     AS "source",
        NULL                      AS "complement_adresse",
        city                      AS "commune",
        street_number_and_address AS "adresse",
        post_code                 AS "code_postal",
        NULL                      AS "code_insee"
    FROM structures
)

SELECT * FROM final
