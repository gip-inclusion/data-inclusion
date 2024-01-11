WITH agences AS (
    SELECT * FROM {{ ref('stg_pole_emploi__agences') }}
),

final AS (
    SELECT
        FALSE                          AS "antenne",
        NULL::TEXT []                  AS "labels_autres",
        NULL::TEXT []                  AS "thematiques",
        NULL                           AS "horaires_ouverture",
        NULL                           AS "lien_source",
        NULL                           AS "presentation_detail",
        NULL                           AS "presentation_resume",
        NULL                           AS "rna",
        'https://www.francetravail.fr' AS "site_web",
        accessibilite                  AS "accessibilite",
        id                             AS "adresse_id",
        courriel                       AS "courriel",
        date_maj                       AS "date_maj",
        id                             AS "id",
        nom                            AS "nom",
        siret                          AS "siret",
        _di_source_id                  AS "source",
        telephone                      AS "telephone",
        'PE'                           AS "typologie",
        ARRAY['pole-emploi']           AS "labels_nationaux"
    FROM agences
)

SELECT * FROM final
