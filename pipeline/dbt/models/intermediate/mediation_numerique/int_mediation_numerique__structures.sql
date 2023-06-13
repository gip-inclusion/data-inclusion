WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

final AS (
    SELECT
        id                 AS "id",
        siret              AS "siret",
        NULL::BOOLEAN      AS "antenne",
        NULL               AS "rna",
        nom                AS "nom",
        commune            AS "commune",
        code_postal        AS "code_postal",
        NULL               AS "code_insee",
        adresse            AS "adresse",
        NULL               AS "complement_adresse",
        longitude          AS "longitude",
        latitude           AS "latitude",
        telephone          AS "telephone",
        courriel           AS "courriel",
        site_web           AS "site_web",
        NULL               AS "lien_source",
        horaires_ouverture AS "horaires_ouverture",
        NULL               AS "accessibilite",
        labels_nationaux   AS "labels_nationaux",
        NULL::TEXT []      AS "labels_autres",
        thematiques        AS "thematiques",
        NULL               AS "typologie",
        NULL               AS "presentation_resume",
        date_maj           AS "date_maj",
        _di_source_id      AS "source",
        NULL               AS "presentation_detail"
    FROM structures
)

SELECT * FROM final
