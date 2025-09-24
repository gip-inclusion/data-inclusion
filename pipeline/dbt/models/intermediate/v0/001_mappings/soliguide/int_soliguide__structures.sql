WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

lieux_with_description AS (
    SELECT
        lieux.*,
        CASE
            WHEN LENGTH(lieux.description) <= 280 THEN lieux.description
            ELSE LEFT(lieux.description, 279) || '…'
        END AS "description_courte",
        CASE
            WHEN lieux.temp_infos__message__name IS NOT NULL AND LENGTH(TRIM(lieux.temp_infos__message__name)) > 1
                THEN 'Information temporaire : ' || lieux.temp_infos__message__name || E'\n'
            ELSE ''
        END || lieux.description || CASE
            WHEN lieux.newhours ->> 'closedHolidays' = 'CLOSED'
                THEN E'\nLa structure est fermée pendant les jours fériés.'
            ELSE ''
        END AS "description_longue"
    FROM lieux
),

final AS (
    SELECT
        lieux.lieu_id                                       AS "id",
        lieux.lieu_id                                       AS "adresse_id",
        NULL                                                AS "rna",
        'soliguide'                                         AS "source",
        NULL                                                AS "accessibilite",
        CAST(NULL AS TEXT [])                               AS "labels_nationaux",
        CAST(NULL AS TEXT [])                               AS "labels_autres",
        CAST(NULL AS TEXT [])                               AS "thematiques",
        NULL                                                AS "typologie",
        lieux.updated_at                                    AS "date_maj",
        NULL                                                AS "siret",
        lieux.name                                          AS "nom",
        lieux.entity_website                                AS "site_web",
        lieux.entity_mail                                   AS "courriel",
        NULL                                                AS "telephone",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url   AS "lien_source",
        processings.soliguide_opening_hours(lieux.newhours) AS "horaires_ouverture",
        lieux.description_courte                            AS "presentation_resume",
        lieux.description_longue                            AS "presentation_detail"
    FROM lieux_with_description AS lieux
    ORDER BY 1
)

SELECT * FROM final
