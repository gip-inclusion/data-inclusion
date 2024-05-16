WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        id                                                                                                                                                                    AS "id",
        id                                                                                                                                                                    AS "adresse_id",
        NULL                                                                                                                                                                  AS "siret",
        NULL::BOOLEAN                                                                                                                                                         AS "antenne",
        NULL                                                                                                                                                                  AS "rna",
        nom                                                                                                                                                                   AS "nom",
        telephone                                                                                                                                                             AS "telephone",
        mail                                                                                                                                                                  AS "courriel",
        details__website                                                                                                                                                      AS "site_web",
        _di_source_id                                                                                                                                                         AS "source",
        'https://monenfant.fr/que-recherchez-vous/' || result_id                                                                                                              AS "lien_source",
        details__infos_pratiques__jour_horaire                                                                                                                                AS "horaires_ouverture",
        NULL                                                                                                                                                                  AS "accessibilite",
        NULL::TEXT []                                                                                                                                                         AS "labels_nationaux",
        NULL::TEXT []                                                                                                                                                         AS "labels_autres",
        CASE WHEN avip THEN 'AVIP' END                                                                                                                                        AS "typologie",
        {{ truncate_text("details__presentation_structure_projet") }}              AS "presentation_resume",
        details__presentation__structure_projet                                                                                                                               AS "presentation_detail",
        derniere_modif_date                                                                                                                                                   AS "date_maj",
        ARRAY['famille--garde-denfants']                                                                                                                                      AS "thematiques"
    FROM creches
)

SELECT * FROM final
