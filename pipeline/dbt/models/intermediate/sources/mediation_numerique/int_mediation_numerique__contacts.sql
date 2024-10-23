WITH final AS (
    SELECT
        id                                      AS "id",
        _di_source_id                           AS "source",
        courriel                                AS "courriel",
        telephone                               AS "telephone",
        NULL                                    AS "contact_nom_prenom"
    FROM {{ ref('stg_mediation_numerique__structures') }}
    WHERE courriel IS NOT NULL
)

SELECT * FROM final
