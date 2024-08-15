WITH final AS (
    SELECT
        courriel                                AS "courriel",
        'mediation-numerique:structures:' || id AS contact_uid
    FROM {{ ref('stg_mediation_numerique__structures') }}
    WHERE courriel IS NOT NULL
)

SELECT * FROM final
