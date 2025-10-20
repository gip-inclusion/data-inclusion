WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        'mes-aides'                           AS "source",
        'mes-aides--' || garages.id           AS "id",
        'mes-aides--' || garages.id           AS "adresse_id",
        garages.nom                           AS "nom",
        garages.modifie_le                    AS "date_maj",
        'https://mes-aides.francetravail.fr/' AS "lien_source",
        garages.siret                         AS "siret",
        garages.telephone                     AS "telephone",
        garages.email                         AS "courriel",
        garages.url                           AS "site_web",
        NULL                                  AS "description",
        NULL                                  AS "horaires_accueil",
        NULL                                  AS "accessibilite_lieu",
        NULL                                  AS "reseaux_porteurs"
    FROM garages
    WHERE
        garages.en_ligne
)

SELECT * FROM final
