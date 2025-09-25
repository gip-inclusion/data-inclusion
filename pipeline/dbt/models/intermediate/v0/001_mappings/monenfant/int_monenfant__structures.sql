WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        structure_id                                                               AS "id",
        structure_id                                                               AS "adresse_id",
        NULL                                                                       AS "siret",
        NULL                                                                       AS "rna",
        structure_name                                                             AS "nom",
        coordonnees__telephone                                                     AS "telephone",
        coordonnees__adresse_mail                                                  AS "courriel",
        coordonnees__site_internet                                                 AS "site_web",
        'monenfant'                                                                AS "source",
        'https://monenfant.fr/que-recherchez-vous/mode-d-accueil/' || structure_id AS "lien_source",
        service_commun__calendrier__jours_horaires_text                            AS "horaires_ouverture",
        NULL                                                                       AS "accessibilite",
        CAST(NULL AS TEXT [])                                                      AS "labels_nationaux",
        CAST(NULL AS TEXT [])                                                      AS "labels_autres",
        CASE WHEN service_commun__avip THEN 'AVIP' END                             AS "typologie",
        CASE
            WHEN LENGTH(description__projet) <= 280 THEN description__projet
            ELSE LEFT(description__projet, 279) || '…'
        END                                                                        AS "presentation_resume",
        description__projet                                                        AS "presentation_detail",
        modified_date                                                              AS "date_maj",
        ARRAY['famille--garde-denfants']                                           AS "thematiques"
    FROM creches
)

SELECT * FROM final
