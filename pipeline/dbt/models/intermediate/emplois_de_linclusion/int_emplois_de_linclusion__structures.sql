WITH siaes AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__siaes') }}
),

organisations AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

structures AS (
    SELECT * FROM siaes
    UNION
    SELECT * FROM organisations
),

final AS (
    SELECT
        id                              AS "id",
        antenne                         AS "antenne",
        longitude                       AS "longitude",
        latitude                        AS "latitude",
        _di_source_id                   AS "source",
        labels_nationaux                AS "labels_nationaux",
        labels_autres                   AS "labels_autres",
        thematiques                     AS "thematiques",
        typologie                       AS "typologie",
        date_maj                        AS "date_maj",
        NULLIF(siret, '')               AS "siret",
        NULLIF(nom, '')                 AS "nom",
        NULLIF(lien_source, '')         AS "lien_source",
        NULLIF(horaires_ouverture, '')  AS "horaires_ouverture",
        NULLIF(accessibilite, '')       AS "accessibilite",
        NULLIF(rna, '')                 AS "rna",
        NULLIF(complement_adresse, '')  AS "complement_adresse",
        NULLIF(presentation_resume, '') AS "presentation_resume",
        NULLIF(presentation_detail, '') AS "presentation_detail",
        NULLIF(adresse, '')             AS "adresse",
        NULLIF(telephone, '')           AS "telephone",
        NULLIF(courriel, '')            AS "courriel",
        NULLIF(site_web, '')            AS "site_web",
        NULLIF(commune, '')             AS "commune",
        NULLIF(code_postal, '')         AS "code_postal",
        NULLIF(code_insee, '')          AS "code_insee"
    FROM structures
)

SELECT * FROM final
