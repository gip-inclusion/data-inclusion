WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
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

sources_mapping AS (
    SELECT
        x.source,
        x.reseau_porteur
    FROM (
        VALUES
        ('Alisol (AD2S)', NULL),
        ('Conseil Départemental du 59', 'departements'),
        ('Croix-Rouge française', 'croix-rouge'),
        ('Crous - Pays de la Loire', NULL),
        ('Société Saint Vincent de Paul', NULL),
        ('restos', 'restos-du-coeur')
    ) AS x (source, reseau_porteur)
),

reseaux_porteurs AS (
    SELECT
        sources.lieu_id,
        ARRAY_AGG(DISTINCT sources_mapping.reseau_porteur) AS reseaux_porteurs
    FROM
        sources
    LEFT JOIN sources_mapping ON sources.source_name = sources_mapping.source
    WHERE sources_mapping.reseau_porteur IS NOT NULL
    GROUP BY sources.lieu_id
),

descriptions AS (
    SELECT
        lieux.id AS lieu_id,
        CASE
            WHEN lieux.temp_infos__message__texte IS NOT NULL AND LENGTH(TRIM(lieux.temp_infos__message__texte)) > 1
                THEN 'Information temporaire : ' || lieux.temp_infos__message__texte || E'\n'
            ELSE ''
        END || lieux.description || CASE
            WHEN lieux.newhours ->> 'closedHolidays' = 'CLOSED'
                THEN E'\nLa structure est fermée pendant les jours fériés.'
            ELSE ''
        END || CASE
            WHEN lieux.modalities__pmr__checked
                THEN '\nLa structure a indiqué être accessible aux personnes à mobilité réduite.'
            ELSE ''
        END      AS "description"
    FROM lieux
),

final AS (
    SELECT
        'soliguide'                                       AS "source",
        'soliguide--' || lieux.id                         AS "id",
        'soliguide--' || lieux.id                         AS "adresse_id",
        lieux.name                                        AS "nom",
        lieux.updated_at                                  AS "date_maj",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url AS "lien_source",
        NULL                                              AS "siret",
        filtered_phones.phone_number                      AS "telephone",
        lieux.entity_mail                                 AS "courriel",
        lieux.entity_website                              AS "site_web",
        CASE
            WHEN LENGTH(descriptions.description) > 2000
                THEN LEFT(descriptions.description, 1999) || '…'
            ELSE descriptions.description
        END                                               AS "description",
        CASE
            WHEN lieux.temp_infos__closure__actif = 'true'
                THEN NULL
            WHEN CAST(lieux.temp_infos__hours -> 'actif' AS BOOLEAN)
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(lieux.temp_infos__hours -> 'hours')
            WHEN lieux.newhours IS NOT NULL
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(lieux.newhours)
        END                                               AS "horaires_accueil",
        NULL                                              AS "accessibilite_lieu",
        reseaux_porteurs.reseaux_porteurs                 AS "reseaux_porteurs"
    FROM lieux
    LEFT JOIN descriptions ON lieux.id = descriptions.lieu_id
    LEFT JOIN filtered_phones ON lieux.id = filtered_phones.lieu_id
    LEFT JOIN reseaux_porteurs ON lieux.id = reseaux_porteurs.lieu_id
)

SELECT * FROM final
