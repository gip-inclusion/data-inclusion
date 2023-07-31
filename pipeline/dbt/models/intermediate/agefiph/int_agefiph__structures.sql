WITH source AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

final AS (
    SELECT
        _di_source_id       AS "source",
        id                  AS "id",
        courriel            AS "courriel",
        nom                 AS "nom",
        commune             AS "commune",
        code_postal         AS "code_postal",
        code_insee          AS "code_insee",
        adresse             AS "adresse",
        complement_adresse  AS "complement_adresse",
        longitude           AS "longitude",
        latitude            AS "latitude",
        typologie           AS "typologie",
        telephone           AS "telephone",
        site_web            AS "site_web",
        presentation_resume AS "presentation_resume",
        presentation_detail AS "presentation_detail",
        date_maj            AS "date_maj",
        antenne             AS "antenne",
        lien_source         AS "lien_source",
        horaires_ouverture  AS "horaires_ouverture",
        accessibilite       AS "accessibilite",
        labels_nationaux    AS "labels_nationaux",
        labels_autres       AS "labels_autres",
        thematiques         AS "thematiques"
    FROM source
)

SELECT * FROM final
