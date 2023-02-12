WITH structures AS (
    SELECT * FROM {{ ref('int__structures') }}
),

plausible_personal_emails AS (
    SELECT * FROM {{ ref('int__plausible_personal_emails') }}
),

deprecated_sirets AS (
    SELECT * FROM {{ ref('int__deprecated_sirets') }}
),

geocoded_results AS (
    SELECT * FROM {{ ref('int_extra__geocoded_results') }}
),

siretisation_annotations AS (
    SELECT * FROM {{ ref('int_siretisation__annotations') }}
),

final AS (
    SELECT
        structures.*,
        deprecated_sirets.sirene_date_fermeture                             AS "sirene_date_fermeture",
        deprecated_sirets.sirene_etab_successeur                            AS "sirene_etab_successeur",
        geocoded_results.result_score                                       AS "geocodage_score",
        geocoded_results.result_citycode                                    AS "geocodage_code_insee",
        siretisation_annotations.siret                                      AS "annotated_siret",
        siretisation_annotations.antenne                                    AS "annotated_antenne",
        COALESCE(plausible_personal_emails.surrogate_id IS NOT NULL, FALSE) AS "email_is_pii",
        COALESCE(deprecated_sirets.surrogate_id IS NOT NULL, FALSE)         AS "has_deprecated_siret"
    FROM
        structures
    LEFT JOIN plausible_personal_emails ON structures.surrogate_id = plausible_personal_emails.surrogate_id
    LEFT JOIN deprecated_sirets ON structures.surrogate_id = deprecated_sirets.surrogate_id
    LEFT JOIN geocoded_results ON structures.surrogate_id = geocoded_results.surrogate_id
    LEFT JOIN siretisation_annotations ON structures.surrogate_id = siretisation_annotations.surrogate_id
)

SELECT * FROM final
