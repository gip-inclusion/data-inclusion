WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_soliguide__adresses_v1') }}
),

phones AS (
    SELECT * FROM {{ ref('stg_soliguide__phones') }}
),

filtered_phones AS (
    -- FIXME: di schema only allows a single phone number, but soliguide can have more
    SELECT DISTINCT ON (lieu_id) *
    FROM phones
),

sources AS (
    SELECT * FROM {{ ref('stg_soliguide__sources') }}
),

reseaux_porteurs AS (
    SELECT
        sources.lieu_id,
        ARRAY_AGG(DISTINCT mapping_.reseau_porteur) AS "reseaux_porteurs"
    FROM
        sources
    INNER JOIN {{ ref('_map_soliguide__reseaux_porteurs_v1') }} AS mapping_
        ON sources.name = mapping_.source
    WHERE mapping_.reseau_porteur IS NOT NULL
    GROUP BY sources.lieu_id
),

final AS (
    SELECT
        'soliguide'                                       AS "source",
        'soliguide--' || lieux.id                         AS "id",
        adresses.id                                       AS "adresse_id",
        lieux.name                                        AS "nom",
        lieux.updated_at                                  AS "date_maj",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url AS "lien_source",
        NULL                                              AS "siret",
        filtered_phones.phone_number                      AS "telephone",
        lieux.entity_mail                                 AS "courriel",
        lieux.entity_website                              AS "site_web",
        ARRAY_TO_STRING(
            ARRAY[
                'Information temporaire : ' || lieux.temp_infos__message__name,
                lieux.description
            ],
            E'\n\n'
        )                                                 AS "description",
        CASE
            WHEN lieux.temp_infos__closure__actif THEN 'closed "fermeture temporaire"'
            WHEN lieux.temp_infos__hours__actif AND lieux.temp_infos__hours__hours IS NOT NULL
                THEN processings.soliguide_opening_hours(lieux.temp_infos__hours__hours)
            WHEN lieux.newhours IS NOT NULL
                THEN processings.soliguide_opening_hours(lieux.newhours)
        END                                               AS "horaires_accueil",
        NULL                                              AS "accessibilite_lieu",
        reseaux_porteurs.reseaux_porteurs                 AS "reseaux_porteurs"
    FROM lieux
    LEFT JOIN adresses ON ('soliguide--' || lieux.id) = adresses.id
    LEFT JOIN filtered_phones ON lieux.id = filtered_phones.lieu_id
    LEFT JOIN reseaux_porteurs ON lieux.id = reseaux_porteurs.lieu_id
)

SELECT * FROM final
