WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        'monenfant'                                                                AS "source",
        'monenfant--' || structure_id                                              AS "id",
        'monenfant--' || structure_id                                              AS "adresse_id",
        structure_name                                                             AS "nom",
        modified_date                                                              AS "date_maj",
        'https://monenfant.fr/que-recherchez-vous/mode-d-accueil/' || structure_id AS "lien_source",
        NULL                                                                       AS "siret",
        coordonnees__telephone                                                     AS "telephone",
        coordonnees__adresse_mail                                                  AS "courriel",
        coordonnees__site_internet                                                 AS "site_web",
        NULLIF(
            ARRAY_TO_STRING(
                ARRAY[
                    '## L’équipe :' || E'\n\n' || description__equipe,
                    '## Le gestionaire :' || E'\n\n' || description__gestionnaire,
                    '## Accueil des enfants en situation d’handicap' || E'\n\n' || service_accueil__handicap
                ],
                E'\n\n'
            ),
            ''
        )                                                                          AS "description",
        processings.monenfant_opening_hours(service_commun__calendrier)            AS "horaires_accueil",
        NULL                                                                       AS "accessibilite_lieu",
        ARRAY['caf']                                                               AS "reseaux_porteurs"
    FROM creches
)

SELECT * FROM final
