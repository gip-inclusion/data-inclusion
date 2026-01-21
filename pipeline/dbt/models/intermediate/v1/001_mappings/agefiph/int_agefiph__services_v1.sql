WITH services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_agefiph__adresses_v1') }}
),

services_thematiques AS (
    SELECT * FROM {{ ref('stg_agefiph__services_thematiques') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

thematiques AS (
    SELECT
        services_thematiques.service_id,
        ARRAY_AGG(DISTINCT mapping.thematique) AS "thematiques"
    FROM services_thematiques
    INNER JOIN {{ ref('_map_agefiph_thematiques_v1') }} AS "mapping"
        ON services_thematiques.thematique_id = mapping.agefiph_thematique_id
    GROUP BY services_thematiques.service_id
),

final AS (
    SELECT
        'agefiph'                                                         AS "source",
        'agefiph--' || structures.id                                      AS "structure_id",
        'agefiph--' || structures.id                                      AS "adresse_id",
        'agefiph--' || structures.id || '-' || services.id                AS "id",
        services.attributes__title                                        AS "nom",
        ARRAY_TO_STRING(
            ARRAY[
                services.attributes__field_solution_detail__value,
                'Aide d’un montant de : ' || LOWER(services.attributes__field_montant_aide)
            ],
            E'\n'
        )                                                                 AS "description",
        'https://www.agefiph.fr' || services.attributes__path__alias      AS "lien_source",
        CAST(services.attributes__changed AS DATE)                        AS "date_maj",
        CASE
            WHEN services.relationships__field_type_de_solution__data__id = 'aec45130-893a-45ba-84e0-41ff7bd99815' THEN 'aide-financiere'
            WHEN services.relationships__field_type_de_solution__data__id = 'f7e83615-cb00-4ddd-91ee-9586d86ccf23' THEN 'accompagnement'
        END                                                               AS "type",
        thematiques.thematiques                                           AS "thematiques",
        'gratuit'                                                         AS "frais",
        NULL                                                              AS "frais_precisions",
        CASE
            -- this case never happens (at the time of writing) but we keep it for safety
            WHEN services.relationships__field_profil_associe__data__id IS NULL THEN ARRAY['tous-publics']
            WHEN services.relationships__field_profil_associe__data__id = '0d0b63b6-4043-4b2d-a3f6-d7c85f335070' THEN ARRAY['personnes-en-situation-de-handicap']
            WHEN services.relationships__field_profil_associe__data__id = '4c67f74c-f56e-49af-9809-805fcf32c23a' THEN ARRAY['actifs']
            WHEN services.relationships__field_profil_associe__data__id = 'df85cc61-d7dc-451f-8cd4-dff0af8dc753' THEN ARRAY['actifs']
        END                                                               AS "publics",
        NULL                                                              AS "publics_precisions",
        NULL                                                              AS "conditions_acces",
        CASE
            WHEN services.attributes__field_lien_aide__uri IS NULL THEN structures.attributes__field_courriel
        END                                                               AS "courriel",
        CASE
            WHEN services.attributes__field_lien_aide__uri IS NULL THEN structures.attributes__field_telephone
        END                                                               AS "telephone",
        ARRAY['a-distance']                                               AS "modes_accueil",
        NULL                                                              AS "contact_nom_prenom",
        CASE WHEN communes.code IS NOT NULL THEN ARRAY[communes.code] END AS "zone_eligibilite",
        services.attributes__field_lien_aide__uri                         AS "lien_mobilisation",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN services.attributes__field_lien_aide__uri IS NULL AND structures.attributes__field_telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN services.attributes__field_lien_aide__uri IS NULL AND structures.attributes__field_courriel IS NOT NULL THEN 'envoyer-un-courriel' END,
                CASE WHEN services.attributes__field_lien_aide__uri IS NOT NULL THEN 'utiliser-lien-mobilisation' END
            ],
            NULL
        )                                                                 AS "modes_mobilisation",
        ARRAY['usagers', 'professionnels']                                AS "mobilisable_par",
        NULL                                                              AS "mobilisation_precisions",
        CAST(NULL AS FLOAT)                                               AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INTEGER)                                             AS "nombre_semaines",
        NULL                                                              AS "horaires_accueil"
    FROM structures
    LEFT JOIN adresses ON ('agefiph--' || structures.id) = adresses.id
    LEFT JOIN communes ON adresses.code_insee = communes.code
    CROSS JOIN services
    LEFT JOIN thematiques ON services.id = thematiques.service_id
    WHERE NOT services.attributes__field_solution_partenaire
)

SELECT * FROM final
