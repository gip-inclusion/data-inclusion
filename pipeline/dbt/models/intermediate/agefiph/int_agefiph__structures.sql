WITH structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

final AS (
    SELECT
        accessibilite         AS "accessibilite",
        id                    AS "adresse_id",
        antenne               AS "antenne",
        courriel              AS "courriel",
        date_maj              AS "date_maj",
        horaires_ouverture    AS "horaires_ouverture",
        id                    AS "id",
        lien_source           AS "lien_source",
        nom                   AS "nom",
        presentation_detail   AS "presentation_detail",
        presentation_resume   AS "presentation_resume",
        NULL                  AS "rna",
        NULL                  AS "siret",
        site_web              AS "site_web",
        _di_source_id         AS "source",
        telephone             AS "telephone",
        typologie             AS "typologie",
        CAST(NULL AS TEXT []) AS "labels_autres",
        CAST(NULL AS TEXT []) AS "labels_nationaux",
        CAST(NULL AS TEXT []) AS "thematiques"
    FROM structures
)

SELECT * FROM final
