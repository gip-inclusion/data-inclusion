WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        id                                                                                                                                                                 AS "id",
        NULL                                                                                                                                                               AS "siret",
        NULL::BOOLEAN                                                                                                                                                      AS "antenne",
        NULL                                                                                                                                                               AS "rna",
        nom                                                                                                                                                                AS "nom",
        ville                                                                                                                                                              AS "commune",
        SUBSTRING(adresse FROM '\d{5}')                                                                                                                                    AS "code_postal",
        NULL                                                                                                                                                               AS "code_insee",
        SUBSTRING(adresse FROM '^(.*?) (- .* )?\d{5}')                                                                                                                     AS "adresse",
        SUBSTRING(adresse FROM '- (.*) \d{5}')                                                                                                                             AS "complement_adresse",
        longitude                                                                                                                                                          AS "longitude",
        latitude                                                                                                                                                           AS "latitude",
        telephone                                                                                                                                                          AS "telephone",
        mail                                                                                                                                                               AS "courriel",
        details_website                                                                                                                                                    AS "site_web",
        _di_source_id                                                                                                                                                      AS "source",
        'https://monenfant.fr/que-recherchez-vous/' || result_id                                                                                                           AS "lien_source",
        details_infos_pratiques_jour_horaire                                                                                                                               AS "horaires_ouverture",
        NULL                                                                                                                                                               AS "accessibilite",
        NULL::TEXT []                                                                                                                                                      AS "labels_nationaux",
        NULL::TEXT []                                                                                                                                                      AS "labels_autres",
        NULL                                                                                                                                                               AS "typologie",
        {{ truncate_text("details_presentation_structure_projet") }}              AS "presentation_resume",
        details_presentation_structure_projet                                                                                                                              AS "presentation_detail",
        derniere_modif_date                                                                                                                                                AS "date_maj",
        ARRAY['famille--garde-denfants']                                                                                                                                   AS "thematiques"
    FROM creches
)

SELECT * FROM final
