WITH final AS (
    SELECT
        id                                      AS "id",
        _di_source_id                           AS "source",
        courriel                                AS "courriel",
        telephone                               AS "telephone",
        contact_nom_prenom                      AS "contact_nom_prenom",
        'mediation-numerique:structures:' || id AS "contact_uid"
    FROM {{ ref('stg_mediation_numerique__structures') }}
    WHERE courriel IS NOT NULL
)

SELECT * FROM final
