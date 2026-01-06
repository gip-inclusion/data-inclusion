WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

services AS (SELECT * FROM {{ ref('stg_fredo__services') }}),

telephones AS (
    SELECT DISTINCT ON (structure_id) *
    FROM {{ ref('stg_fredo__telephones') }}
    ORDER BY structure_id
),

emails AS (
    SELECT DISTINCT ON (structure_id) *
    FROM {{ ref('stg_fredo__emails') }}
    ORDER BY structure_id
),

frais AS (
    SELECT DISTINCT ON (structure_id)
        structure_id,
        CASE
            WHEN value = 'gratuit' THEN 'gratuit'
            WHEN value = 'payant' THEN 'payant'
            WHEN value !~* 'condition' THEN 'gratuit'
        END AS "frais"
    FROM {{ ref('stg_fredo__frais') }}
    WHERE value IS NOT NULL
    ORDER BY structure_id, value = 'gratuit' DESC
),

frais_precisions AS (
    SELECT
        structure_id,
        STRING_AGG(
            CASE
                WHEN value != 'gratuit' AND value != 'payant' AND value !~* 'condition'
                    THEN value
            END,
            ', '
        ) AS "frais_precisions"
    FROM {{ ref('stg_fredo__frais') }}
    -- these values often contain appointment
    -- details unrelated to fees : filter them out
    WHERE value !~* 'rendez'
    GROUP BY structure_id
),

publics AS (
    SELECT
        publics.structure_id,
        CASE
            WHEN 'tous-publics' = ANY(ARRAY_AGG(mapping.public_di))
                THEN ARRAY['tous-publics']
            ELSE ARRAY_AGG(DISTINCT mapping.public_di ORDER BY mapping.public_di)
        END AS "publics"
    FROM {{ ref('stg_fredo__publics') }} AS publics
    INNER JOIN {{ ref('_map_fredo__publics') }} AS "mapping"
        ON publics.value = mapping.public_fredo
    GROUP BY publics.structure_id
),

thematiques AS (
    SELECT
        categories.structure_id,
        ARRAY_AGG(DISTINCT mapping.thematique ORDER BY mapping.thematique) AS "thematiques"
    FROM {{ ref('stg_fredo__categories') }} AS categories
    INNER JOIN {{ ref('_map_fredo__thematiques') }} AS "mapping"
        ON categories.value = mapping.category
    GROUP BY categories.structure_id
),

final AS (
    SELECT
        'fredo'                                                            AS "source",
        'fredo--' || structures.id || '-' || services.value                AS "id",
        'fredo--' || structures.id                                         AS "adresse_id",
        'fredo--' || structures.id                                         AS "structure_id",
        UPPER(LEFT(services.value, 1)) || SUBSTRING(services.value FROM 2) AS "nom",
        structures.presentation_resume                                     AS "description",
        'https://fredo.re/ad/' || SLUGIFY(structures.nom)                  AS "lien_source",
        CAST(structures.last_update AS DATE)                               AS "date_maj",
        mapping_type.type                                                  AS "type",
        thematiques.thematiques                                            AS "thematiques",
        frais.frais                                                        AS "frais",
        frais_precisions.frais_precisions                                  AS "frais_precisions",
        publics.publics                                                    AS "publics",
        NULL                                                               AS "conditions_acces",
        telephones.value                                                   AS "telephone",
        emails.value                                                       AS "courriel",
        NULL                                                               AS "contact_nom_prenom",
        CASE
            WHEN structures.adresse IS NOT NULL
                THEN ARRAY['en-presentiel']
            ELSE ARRAY['a-distance']
        END                                                                AS "modes_accueil",
        ARRAY['974']                                                       AS "zone_eligibilite",
        NULL                                                               AS "lien_mobilisation",
        ARRAY['envoyer-un-courriel', 'telephoner']                         AS "modes_mobilisation",
        ARRAY['professionnels', 'usagers']                                 AS "mobilisable_par",
        NULL                                                               AS "mobilisation_precisions",
        NULL                                                               AS "volume_horaire_hebdomadaire",
        NULL                                                               AS "nombre_semaines",
        structures.horaires_ouverture                                      AS "horaires_accueil"
    FROM services
    LEFT JOIN structures ON services.structure_id = structures.id
    LEFT JOIN telephones ON structures.id = telephones.structure_id
    LEFT JOIN emails ON structures.id = emails.structure_id
    LEFT JOIN publics ON structures.id = publics.structure_id
    LEFT JOIN frais ON structures.id = frais.structure_id
    LEFT JOIN frais_precisions ON structures.id = frais_precisions.structure_id
    LEFT JOIN thematiques ON structures.id = thematiques.structure_id
    LEFT JOIN {{ ref('_map_fredo__types') }} AS mapping_type
        ON services.value = mapping_type.service
)

SELECT * FROM final
