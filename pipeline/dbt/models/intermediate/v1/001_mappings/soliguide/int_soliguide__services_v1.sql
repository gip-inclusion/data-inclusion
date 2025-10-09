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
    SELECT * FROM {{ ref('int_soliguide__publics') }}
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
        NULLIF(TRIM(BOTH E'\n ' FROM CASE
            WHEN LENGTH(services.description) <= 280 THEN services.description
            ELSE LEFT(services.description, 279) || '…'
        END), '')   AS "description_courte",
        NULLIF(TRIM(BOTH E'\n ' FROM CASE
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
        END), '')   AS "description"
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

filtered_thematiques AS (
    SELECT *
    FROM thematiques
    WHERE
        thematiques IS NOT NULL
        AND CARDINALITY(thematiques) != 0
),

types AS (
    SELECT DISTINCT
        services.id              AS "id",
        thematiques_mapping.type AS "type"
    FROM
        open_services AS services
    LEFT JOIN thematiques_mapping ON services.category = thematiques_mapping.category

),

filtered_types AS (
    SELECT *
    FROM types
    WHERE type IS NOT NULL
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
        filtered_types.type                               AS "type",
        thematiques.thematiques                           AS "thematiques",
        CASE
            WHEN services.modalities__price__checked THEN 'payant'
            ELSE 'gratuit'
        END                                               AS "frais",
        services.modalities__price__precisions            AS "frais_precisions",

        CASE
            WHEN lieux.publics__accueil IN (0, 1) THEN ARRAY['tous-publics']
            ELSE publics.publics
        END                                               AS "publics",
        NULLIF(TRIM(
            BOTH E'\n ' FROM COALESCE(services.data #>> '{publics,description}', '', lieux.publics__precisions, '')
            || CASE
                WHEN lieux.publics__age__min IS NOT NULL AND lieux.publics__age__min != 0
                    THEN FORMAT(E'\nL’âge minimum est de %s ans.', lieux.publics__age__min)
                ELSE ''
            END
            || CASE
                WHEN lieux.publics__age__max IS NOT NULL AND lieux.publics__age__max != 99
                    THEN FORMAT(E'L’âge maximum est de %s ans.', lieux.publics__age__max)
                ELSE ''
            END
        ), '')                                            AS "publics_precisions",
        -- modalities.other is always enclosed within <xxx> HTML tags.
        NULLIF(TRIM(BOTH E'\n ' FROM REGEXP_REPLACE(CASE
            WHEN services.data #> '{modalities,other}' IS NOT NULL
                THEN
                    services.data #>> '{modalities,other}'
            ELSE lieux.modalities__other
        END, '<[^>]*>', '', 'g')), '')                    AS "conditions_acces",
        filtered_phones.phone_number                      AS "telephone",
        lieux.entity_mail                                 AS "courriel",
        NULL                                              AS "contact_nom_prenom",
        ARRAY['en-presentiel']                            AS "modes_accueil",
        -- Le département n'est pas encore "résolu" par la BAN donc je préfère éviter une jointure imprécise ici ?
        -- S'assurer qu'il est mis en place plus tard via le type de Zone Eligibilité ?
        CAST(NULL AS TEXT [])                             AS "zone_eligibilite",
        'departement'                                     AS "zone_eligibilite_type",
        NULL                                              AS "lien_mobilisation",
        CASE
            WHEN services.data ->> 'differentModalities' = 'true'
                THEN CASE
                    WHEN services.data #>> '{modalities,inconditionnel,checked}' = 'true'
                        THEN ARRAY['envoyer-un-courriel', 'telephoner', 'se-presenter']
                    WHEN services.data #>> '{modalities,appointment,checked}' = 'true'
                        THEN ARRAY['envoyer-un-courriel', 'telephoner']
                    WHEN services.data #>> '{modalities,inscription,checked}' = 'true'
                        THEN ARRAY['envoyer-un-courriel', 'telephoner']
                    WHEN services.data #>> '{modalities,orientation,checked}' = 'true'
                        THEN ARRAY['envoyer-un-courriel', 'telephoner']
                END
            WHEN lieux.modalities__inconditionnel
                THEN ARRAY['envoyer-un-courriel', 'telephoner', 'se-presenter']
            WHEN lieux.modalities__appointment__checked
                THEN ARRAY['envoyer-un-courriel', 'telephoner']
            WHEN lieux.modalities__inscription__checked
                THEN ARRAY['envoyer-un-courriel', 'telephoner']
            WHEN lieux.modalities__orientation__checked
                THEN ARRAY['envoyer-un-courriel', 'telephoner']
        END                                               AS "modes_mobilisation",
        ARRAY['usagers', 'professionnels']                AS "mobilisable_par",
        NULLIF(TRIM(
            BOTH E'\n ' FROM
            COALESCE(services.data #>> '{modalities,appointment,precisions}', '') || ' '
            || COALESCE(services.data #>> '{modalities,inscription,precisions}', '') || ' '
            || COALESCE(services.data #>> '{modalities,orientation,precisions}', '')
        ),
        '')                                               AS "mobilisation_precisions",
        CAST(NULL AS FLOAT)                               AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INTEGER)                             AS "nombre_semaines",
        CASE
            WHEN lieux.temp_infos__closure__actif = 'true'
                THEN NULL
            WHEN services.data ->> 'differentHours' = 'true'
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(services.hours)
            WHEN CAST(lieux.temp_infos__hours -> 'actif' AS BOOLEAN)
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(lieux.temp_infos__hours -> 'hours')
            WHEN lieux.newhours IS NOT NULL
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(lieux.newhours)
        END                                               AS "horaires_accueil"
    FROM open_services AS services
    LEFT JOIN descriptions ON services.id = descriptions.id
    LEFT JOIN lieux ON services.lieu_id = lieux.id
    LEFT JOIN publics ON services.lieu_id = publics.lieu_id
    LEFT JOIN categories ON services.category = categories.code
    LEFT JOIN filtered_types ON services.id = filtered_types.id
    LEFT JOIN filtered_thematiques AS thematiques ON services.id = thematiques.id
    LEFT JOIN filtered_phones ON services.lieu_id = filtered_phones.lieu_id
)

SELECT * FROM final
