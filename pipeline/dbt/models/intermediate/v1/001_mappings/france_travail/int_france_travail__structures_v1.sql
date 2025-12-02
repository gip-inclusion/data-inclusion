WITH agences AS (
    SELECT * FROM {{ ref('stg_france_travail__agences') }}
),

final AS (
    SELECT
        'france-travail'                                   AS "source",
        'france-travail--' || code                         AS "id",
        'france-travail--' || code                         AS "adresse_id",
        libelle_etendu                                     AS "nom",
        CURRENT_DATE                                       AS "date_maj",
        NULL                                               AS "lien_source",
        siret                                              AS "siret",
        '3949'                                             AS "telephone",
        -- FT does not want us to use email addresses of their agencies
        NULL                                               AS "courriel",
        'https://www.francetravail.fr'                     AS "site_web",
        NULL                                               AS "description",
        processings.france_travail_opening_hours(horaires) AS "horaires_accueil",
        CASE
            WHEN dispositif_adeda
                THEN 'https://www.francetravail.fr/actualites/a-laffiche/2022/adeda-un-dispositif-pour-mieux-a.html'
        END                                                AS "accessibilite_lieu",
        ARRAY['france-travail']                            AS "reseaux_porteurs"
    FROM agences
)

SELECT * FROM final
