WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

services AS (
    SELECT * FROM {{ ref('stg_soliguide__services') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_soliguide__categories') }}
),

phones AS (
    SELECT * FROM {{ ref('stg_soliguide__phones') }}
),

thematiques_mapping AS (
    SELECT * FROM {{ ref('_map_soliguide_thematiques_v1') }}
),

publics AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__administrative') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__gender') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__familiale') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__other') }}
),

filtered_phones AS (
    -- FIXME: di schema only allows a single phone number, but soliguide can have more
    SELECT DISTINCT ON (lieu_id) *
    FROM phones
),

open_services AS (
    SELECT *
    FROM services
    WHERE
        NOT close__actif
        OR
        (close__date_debut > CURRENT_DATE AT TIME ZONE 'Europe/Paris')
        OR
        (close__date_fin < CURRENT_DATE AT TIME ZONE 'Europe/Paris')
),

descriptions AS (
    SELECT
        services.id AS "id",
        CASE
            WHEN LENGTH(services.description) <= 280 THEN services.description
            ELSE LEFT(services.description, 279) || '…'
        END         AS "description_courte",
        CASE
            WHEN services.close__actif AND services.close__date_fin IS NULL
                THEN 'Ce service est fermé temporairement depuis le ' || TO_CHAR(services.close__date_debut, 'DD/MM/YYYY') || E'.\n'
            WHEN services.close__actif AND services.close__date_fin IS NOT NULL
                THEN 'Ce service est fermé temporairement du ' || TO_CHAR(services.close__date_debut, 'DD/MM/YYYY') || ' au ' || TO_CHAR(services.close__date_fin, 'DD/MM/YYYY') || E'.\n'
            ELSE ''
        END || CASE
            WHEN services.saturated__status = 'HIGH'
                THEN E'Attention, la structure est très sollicitée pour ce service.\n'
            ELSE ''
        END || COALESCE(services.description, lieux.description) || CASE
            WHEN services.hours ->> 'closedHolidays' = 'CLOSED'
                THEN E'\nLa structure est fermée pendant les jours fériés.'
            ELSE ''
        END         AS "description"
    FROM open_services AS services
    LEFT JOIN lieux ON services.lieu_id = lieux.id
),

thematiques AS (
    SELECT
        services.id                                                            AS "id",
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT thematiques_mapping.thematique), NULL) AS "thematiques"
    FROM
        open_services AS services
    LEFT JOIN thematiques_mapping ON services.category = thematiques_mapping.category
    GROUP BY
        services.id
),

types AS (
    SELECT DISTINCT
        services.id              AS "id",
        thematiques_mapping.type AS "type"
    FROM
        open_services AS services
    LEFT JOIN thematiques_mapping ON services.category = thematiques_mapping.category

),

final AS (
    SELECT
        'soliguide'                                       AS "source",
        'soliguide--' || services.id                      AS "id",
        'soliguide--' || lieux.id                         AS "adresse_id",
        'soliguide--' || lieux.id                         AS "structure_id",
        categories.label                                  AS "nom",
        CASE
            WHEN LENGTH(descriptions.description) > 2000 THEN LEFT(descriptions.description, 1999) || '…'
            ELSE descriptions.description
        END                                               AS "description",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url AS "lien_source",
        services.updated_at                               AS "date_maj",
        types.type                                        AS "type",
        thematiques.thematiques                           AS "thematiques",
        CASE
            WHEN services.modalities__price__checked THEN 'payant'
            ELSE 'gratuit'
        END                                               AS "frais",
        services.modalities__price__precisions            AS "frais_precisions",

        -- TODO: IAMHERE
        CAST(NULL AS TEXT [])                             AS "publics",
        NULL                                              AS "publics_precisions",
        NULL                                              AS "conditions_acces",
        NULL                                              AS "telephone",
        NULL                                              AS "courriel",
        CAST(NULL AS TEXT [])                             AS "modes_accueil",
        CAST(NULL AS TEXT [])                             AS "zone_eligibilite",
        'departement'                                     AS "zone_eligibilite_type",
        NULL                                              AS "contact_nom_prenom",
        NULL                                              AS "lien_mobilisation",
        CAST(NULL AS TEXT [])                             AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                             AS "mobilisable_par",
        NULL                                              AS "mobilisation_precisions",
        CAST(NULL AS FLOAT)                               AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INTEGER)                             AS "nombre_semaines",
        NULL                                              AS "horaires_accueil"
    FROM open_services AS services
    LEFT JOIN descriptions ON services.id = descriptions.id
    LEFT JOIN lieux ON services.lieu_id = lieux.id
    LEFT JOIN categories ON services.category = categories.code
    LEFT JOIN types ON services.id = types.id AND types.type IS NOT NULL
    INNER JOIN thematiques ON services.id = thematiques.id AND (
        thematiques.thematiques IS NOT NULL
        AND CARDINALITY(thematiques.thematiques) != 0
    )
)

SELECT * FROM final
