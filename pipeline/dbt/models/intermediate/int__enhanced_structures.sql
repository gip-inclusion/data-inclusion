WITH structures AS (
    SELECT * FROM {{ ref('int__structures') }}
),

plausible_personal_emails AS (
    SELECT * FROM {{ ref('int__plausible_personal_emails') }}
),

deprecated_sirets AS (
    SELECT * FROM {{ ref('int__deprecated_sirets') }}
),

adresses_geocoded AS (
    SELECT * FROM {{ ref('int__adresses_geocoded') }}
),

siretisation_annotations AS (
    SELECT * FROM {{ ref('int_siretisation__annotations') }}
),

final AS (
    SELECT
        structures.*,
        deprecated_sirets.sirene_date_fermeture                                 AS "_di_sirene_date_fermeture",
        deprecated_sirets.sirene_etab_successeur                                AS "_di_sirene_etab_successeur",
        adresses_geocoded.longitude                                             AS "longitude",
        adresses_geocoded.latitude                                              AS "latitude",
        adresses_geocoded.complement_adresse                                    AS "complement_adresse",
        adresses_geocoded.commune                                               AS "commune",
        adresses_geocoded.adresse                                               AS "adresse",
        adresses_geocoded.code_postal                                           AS "code_postal",
        adresses_geocoded.code_insee                                            AS "code_insee",
        adresses_geocoded.result_score                                          AS "_di_geocodage_score",
        adresses_geocoded.result_citycode                                       AS "_di_geocodage_code_insee",
        siretisation_annotations.siret                                          AS "_di_annotated_siret",
        siretisation_annotations.antenne                                        AS "_di_annotated_antenne",
        COALESCE(plausible_personal_emails._di_surrogate_id IS NOT NULL, FALSE) AS "_di_email_is_pii",
        COALESCE(deprecated_sirets._di_surrogate_id IS NOT NULL, FALSE)         AS "_di_has_deprecated_siret"
    FROM
        structures
    LEFT JOIN plausible_personal_emails ON structures._di_surrogate_id = plausible_personal_emails._di_surrogate_id
    LEFT JOIN deprecated_sirets ON structures._di_surrogate_id = deprecated_sirets._di_surrogate_id
    LEFT JOIN adresses_geocoded ON structures._di_adresse_surrogate_id = adresses_geocoded._di_surrogate_id
    LEFT JOIN siretisation_annotations ON structures._di_surrogate_id = siretisation_annotations._di_surrogate_id
)

SELECT * FROM final
