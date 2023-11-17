WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

plausible_personal_emails AS (
    SELECT * FROM {{ ref('int__plausible_personal_emails') }}
),

deprecated_sirets AS (
    SELECT * FROM {{ ref('int__deprecated_sirets') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__union_adresses__enhanced') }}
),

valid_structures AS (
    SELECT structures.*
    FROM structures
    LEFT JOIN LATERAL
        LIST_STRUCTURE_ERRORS(
            accessibilite,
            antenne,
            courriel,
            date_maj,
            horaires_ouverture,
            id,
            labels_autres,
            labels_nationaux,
            lien_source,
            nom,
            presentation_detail,
            presentation_resume,
            rna,
            siret,
            site_web,
            source,
            telephone,
            thematiques,
            typologie
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
),

final AS (
    SELECT
        valid_structures.*,
        deprecated_sirets.sirene_date_fermeture                                 AS "_di_sirene_date_fermeture",
        deprecated_sirets.sirene_etab_successeur                                AS "_di_sirene_etab_successeur",
        adresses.longitude                                                      AS "longitude",
        adresses.latitude                                                       AS "latitude",
        adresses.complement_adresse                                             AS "complement_adresse",
        adresses.commune                                                        AS "commune",
        adresses.adresse                                                        AS "adresse",
        adresses.code_postal                                                    AS "code_postal",
        adresses.code_insee                                                     AS "code_insee",
        adresses.result_score                                                   AS "_di_geocodage_score",
        adresses.result_citycode                                                AS "_di_geocodage_code_insee",
        COALESCE(plausible_personal_emails._di_surrogate_id IS NOT NULL, FALSE) AS "_di_email_is_pii"
    FROM
        valid_structures
    LEFT JOIN plausible_personal_emails ON valid_structures._di_surrogate_id = plausible_personal_emails._di_surrogate_id
    LEFT JOIN deprecated_sirets ON valid_structures._di_surrogate_id = deprecated_sirets._di_surrogate_id
    LEFT JOIN adresses ON valid_structures._di_adresse_surrogate_id = adresses._di_surrogate_id
)

SELECT * FROM final
