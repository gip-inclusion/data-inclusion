WITH final AS (
    SELECT
        id                                      AS "id",
        _di_source_id                           AS "source",
        courriel                                AS "courriel",
        telephone                               AS "telephone",
        NULL                                    AS "contact_nom_prenom",
        -- services in mediation numerique have no contact information
        'mediation-numerique:structures:' || id AS "contact_uid"
    FROM {{ ref('stg_mediation_numerique__structures') }}
    WHERE courriel IS NOT NULL
)

SELECT * FROM final
