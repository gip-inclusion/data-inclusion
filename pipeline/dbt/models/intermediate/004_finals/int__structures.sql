WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__adresses') }}
),

valid_site_web AS (
    SELECT
        input_url,
        "url"
    FROM {{ ref('int__urls') }}
    WHERE status_code > 0
)

SELECT
    structures._di_surrogate_id                           AS "_di_surrogate_id",
    structures.accessibilite                              AS "accessibilite",
    structures.courriel                                   AS "courriel",
    structures.horaires_ouverture                         AS "horaires_ouverture",
    structures.id                                         AS "id",
    structures.labels_autres                              AS "labels_autres",
    structures.labels_nationaux                           AS "labels_nationaux",
    structures.lien_source                                AS "lien_source",
    structures.presentation_detail                        AS "presentation_detail",
    structures.presentation_resume                        AS "presentation_resume",
    structures.rna                                        AS "rna",
    structures.siret                                      AS "siret",
    structures.source                                     AS "source",
    structures.typologie                                  AS "typologie",
    structures.date_maj                                   AS "date_maj",
    structures.thematiques                                AS "thematiques",
    processings.format_phone_number(structures.telephone) AS "telephone",
    valid_site_web.url                                    AS "site_web",
    CASE
        WHEN LENGTH(structures.nom) <= 150 THEN structures.nom
        ELSE LEFT(structures.nom, 149) || '…'
    END                                                   AS "nom",
    adresses.longitude                                    AS "longitude",
    adresses.latitude                                     AS "latitude",
    adresses.complement_adresse                           AS "complement_adresse",
    adresses.commune                                      AS "commune",
    adresses.adresse                                      AS "adresse",
    adresses.code_postal                                  AS "code_postal",
    adresses.code_insee                                   AS "code_insee"
FROM structures
LEFT JOIN valid_site_web ON structures.site_web = valid_site_web.input_url
LEFT JOIN adresses ON structures._di_adresse_surrogate_id = adresses._di_surrogate_id
