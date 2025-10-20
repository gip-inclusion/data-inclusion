WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

garages__besoins AS (
    SELECT DISTINCT ON (garage_id) *
    FROM {{ ref('stg_mes_aides__garages__besoins') }}
    ORDER BY garage_id ASC, item ASC
),

final AS (
    SELECT
        'mes-aides'                                         AS "source",
        'mes-aides--' || garages.id                         AS "id",
        'mes-aides--' || garages.id                         AS "adresse_id",
        garages.nom                                         AS "nom",
        COALESCE(garages.mis_a_jour_le, garages.modifie_le) AS "date_maj",
        FORMAT(
            'https://mes-aides.francetravail.fr/%s/%s/%s/%s',
            REGEXP_REPLACE(LOWER(UNACCENT(garages__besoins.categorie)), '\W', '-', 'g'),
            REGEXP_REPLACE(LOWER(UNACCENT(garages__besoins.item)), '\W', '-', 'g'),
            REGEXP_REPLACE(LOWER(UNACCENT(garages.partenaire)), '\W', '-', 'g'),
            REGEXP_REPLACE(LOWER(UNACCENT(garages.nom)), '\W', '-', 'g')
        )                                                   AS "lien_source",
        NULL                                                AS "siret",
        garages.telephone                                   AS "telephone",
        garages.email                                       AS "courriel",
        garages.url                                         AS "site_web",
        NULL                                                AS "description",
        NULL                                                AS "horaires_accueil",
        NULL                                                AS "accessibilite_lieu",
        CASE garages.partenaire
            WHEN 'AGIL''ESS' THEN ARRAY['agil-ess']
        END                                                 AS "reseaux_porteurs"
    FROM garages
    LEFT JOIN garages__besoins ON garages.id = garages__besoins.garage_id
    WHERE
        garages.en_ligne
)

SELECT * FROM final
