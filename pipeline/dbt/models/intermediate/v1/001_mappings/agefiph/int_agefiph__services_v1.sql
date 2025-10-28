WITH services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int_agefiph__structures') }}
),

structures_staging AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

services_thematiques AS (
    SELECT * FROM {{ ref('stg_agefiph__services_thematiques') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

code_insee AS (
    SELECT DISTINCT ON (1)
        structures_staging.attributes__field_adresse__locality                  AS input_commune,
        structures_staging.attributes__field_adresse__postal_code               AS input_code_postal,
        communes.nom,
        communes.codes_postaux,
        communes.nom <-> structures_staging.attributes__field_adresse__locality AS distance,
        communes.code_region,
        communes.nom_region
    FROM structures_staging
    LEFT JOIN communes
        ON (communes.nom || ' ' || LEFT(communes.code, 2)) % (REPLACE(LOWER(structures_staging.attributes__field_adresse__locality), 'cedex', '') || ' ' || LEFT(structures_staging.attributes__field_adresse__postal_code, 2))
    ORDER BY structures_staging.attributes__field_adresse__locality, communes.nom <-> structures_staging.attributes__field_adresse__locality ASC
),

agefiph_thematique_mapping AS (
    SELECT * FROM {{ ref('_map_agefiph_thematiques_v1') }}
),

final AS (
    SELECT
        'agefiph'                                                    AS "source",
        'agefiph--' || structures.id                                 AS "structure_id",
        'agefiph--' || structures.id                                 AS "adresse_id",
        'agefiph--' || structures.id || '-' || services.id           AS "id",
        services.attributes__title                                   AS "nom",
        ARRAY_TO_STRING(
            ARRAY[
                services.attributes__field_solution_detail__processed,
                '\Aide d’un montant de : ' || LOWER(services.attributes__field_montant_aide)
            ],
            E'\n'
        )                                                            AS "description",
        'https://www.agefiph.fr' || services.attributes__path__alias AS "lien_source",
        CAST(services.attributes__changed AS DATE)                   AS "date_maj",
        CASE
            WHEN services.relationships__field_type_de_solution__data__id = 'aec45130-893a-45ba-84e0-41ff7bd99815' THEN 'aide-financiere'
            WHEN services.relationships__field_type_de_solution__data__id = 'f7e83615-cb00-4ddd-91ee-9586d86ccf23' THEN 'accompagnement'
        END                                                          AS "type",
        ARRAY(
            SELECT DISTINCT agefiph_thematique_mapping.thematique
            FROM services_thematiques
            INNER JOIN agefiph_thematique_mapping ON services_thematiques.thematique_id = agefiph_thematique_mapping.agefiph_thematique_id
            WHERE services.id = services_thematiques.service_id
        )                                                            AS "thematiques",
        'gratuit'                                                    AS "frais",
        NULL                                                         AS "frais_precisions",
        CASE
            -- this case never happens (at the time of writing) but we keep it for safety
            WHEN services.relationships__field_profil_associe__data__id IS NULL THEN ARRAY['tous-publics']
            WHEN services.relationships__field_profil_associe__data__id = '0d0b63b6-4043-4b2d-a3f6-d7c85f335070' THEN ARRAY['personnes-en-situation-de-handicap']
            WHEN services.relationships__field_profil_associe__data__id = '4c67f74c-f56e-49af-9809-805fcf32c23a' THEN ARRAY['actifs']
            WHEN services.relationships__field_profil_associe__data__id = 'df85cc61-d7dc-451f-8cd4-dff0af8dc753' THEN ARRAY['actifs']
        END                                                          AS "publics",
        NULL                                                         AS "publics_precisions",
        NULL                                                         AS "conditions_acces",
        CASE
            WHEN services.attributes__field_lien_aide_uri IS NULL THEN structures.courriel
        END                                                          AS "courriel",
        CASE
            WHEN services.attributes__field_lien_aide_uri IS NULL THEN structures.telephone
        END                                                          AS "telephone",
        ARRAY['a-distance']                                          AS "modes_accueil",
        NULL                                                         AS "contact_nom_prenom",
        CASE
            WHEN code_insee.code_region IS NULL THEN NULL ELSE
                ARRAY[CAST(code_insee.code_region AS TEXT)]
        END                                                          AS "zone_eligibilite",
        REPLACE(
            services.attributes__field_lien_aide_uri,
            'internal:',
            'https://www.agefiph.fr'
        )                                                            AS "lien_mobilisation",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.courriel IS NOT NULL THEN 'envoyer-un-courriel' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NOT NULL THEN 'utiliser-lien-mobilisation' END
            ],
            NULL
        )                                                            AS "modes_mobilisation",
        ARRAY['usagers', 'professionnels']                           AS "mobilisable_par",
        NULL                                                         AS "mobilisation_precisions",
        CAST(NULL AS INT)                                            AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                            AS "nombre_semaines",
        NULL                                                         AS "horaires_accueil"

    FROM
        structures
    CROSS JOIN services
    INNER JOIN structures_staging ON structures.id = structures_staging.id
    LEFT JOIN code_insee
        ON
            structures_staging.attributes__field_adresse__locality = code_insee.input_commune
            AND structures_staging.attributes__field_adresse__postal_code = code_insee.input_code_postal
            AND code_insee.distance < 0.6
    WHERE services.attributes__field_solution_partenaire = 'false'
)

SELECT * FROM final
