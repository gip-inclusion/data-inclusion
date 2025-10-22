WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        'monenfant'                   AS "source",
        'monenfant--' || structure_id AS "id",
        'monenfant--' || structure_id AS "adresse_id",
        structure_name                AS "nom",
        modified_date                 AS "date_maj",
        FORMAT(
            'https://monenfant.fr/que-recherchez-vous/%s/%s/%s',
            structure_name,
            structure_type,
            structure_id
        )                             AS "lien_source",
        NULL                          AS "siret",
        coordonnees__telephone        AS "telephone",
        coordonnees__adresse_mail     AS "courriel",
        coordonnees__site_internet    AS "site_web",
        NULLIF(
            ARRAY_TO_STRING(
                ARRAY[
                    '## Le gestionaire :' || E'\n\n' || description__gestionnaire,
                    '## L’équipe :' || E'\n\n' || description__equipe
                ],
                E'\n\n'
            ),
            ''
        )                             AS "description",
        NULL                          AS "horaires_accueil",  -- TODO
        NULL                          AS "accessibilite_lieu",
        ARRAY['caf']                  AS "reseaux_porteurs"
    FROM creches
)

SELECT * FROM final
