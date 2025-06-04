WITH services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int_agefiph__structures') }}
),

structures_staging AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

-- https://www.agefiph.fr/jsonapi/taxonomy_term/type_aide_service
di_type_by_agefiph_type AS (
    SELECT x.*
    FROM (
        VALUES
        ('aec45130-893a-45ba-84e0-41ff7bd99815', 'aide-financiere'),
        ('f7e83615-cb00-4ddd-91ee-9586d86ccf23', 'accompagnement')
    ) AS x (agefiph_type, type_)
),

services_thematiques AS (
    SELECT * FROM {{ ref('stg_agefiph__services_thematiques') }}
),

di_profils_by_agefiph_profil AS (
    SELECT x.*
    FROM (
        VALUES
        ('0d0b63b6-4043-4b2d-a3f6-d7c85f335070', ARRAY['personnes-en-situation-de-handicap'], NULL),
        ('4c67f74c-f56e-49af-9809-805fcf32c23a', ARRAY['salaries'], 'employeur'),
        ('df85cc61-d7dc-451f-8cd4-dff0af8dc753', ARRAY['salaries'], 'acteur de la formation')
    ) AS x (agefiph_profil, profil, profil_precision)
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

-- https://www.agefiph.fr/jsonapi/taxonomy_term/thematique
di_thematique_by_agefiph_thematique AS (
    SELECT x.*
    FROM (
        VALUES
        ('4e08047f-b0ed-431a-9182-61e8e61b1486', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('907a8c33-5c56-49d3-bd64-a736a9ceac76', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('11618ce3-e59b-404f-8eb2-5763215464f2', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('60c25ci7-61sc-89a9-ny54-126hslf808a2', 'handicap--connaissance-des-droits-des-travailleurs'),
        ('cb2c9fec-c190-4e2f-aeee-6da818109bf8', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('78b28acb-803e-4b06-ab77-58dabfbd8571', 'handicap--adaptation-au-poste-de-travail'),
        ('fb5e6180-290b-4216-ba68-624d25defa3a', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('03228d62-2a59-49d8-8443-b25cb2e684b9', 'accompagnement-social-et-professionnel-personnalise--definition-du-projet-professionnel'),
        ('f9ab3e06-af51-463a-aaf7-7b04a28e047f', 'se-former--trouver-sa-formation'),
        ('aeab1d68-4e89-4e2a-a612-d8645e3999d8', 'creation-activite--definir-son-projet-de-creation-dentreprise'),
        ('f4551558-8315-4708-8357-5ecc89751bc6', 'handicap--faire-reconnaitre-un-handicap'),
        ('9d609684-2597-4916-a897-753cfb0e8bc8', 'handicap--aide-a-la-personne'),
        ('5d8c88d8-db03-4f27-b517-d7016896b01a', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'handicap--connaissance-des-droits-des-travailleurs'),
        ('ddf0fa87-2ee0-481c-a258-96985b7826c3', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi')
    ) AS x (agefiph_thematique_id, thematique)
),

final AS (
    SELECT
        structures.id                                                AS "adresse_id",
        NULL                                                         AS "contact_nom_prenom",
        CASE
            WHEN services.attributes__field_lien_aide_uri IS NULL THEN structures.courriel
        END                                                          AS "courriel",
        REPLACE(
            services.attributes__field_lien_aide_uri,
            'internal:',
            'https://www.agefiph.fr'
        )                                                            AS "formulaire_en_ligne",
        NULL                                                         AS "frais_autres",
        services.attributes__title                                   AS "nom",
        CASE
            WHEN LENGTH(services.attributes__field_solution_detail__processed) <= 280 THEN services.attributes__field_solution_detail__processed
            ELSE LEFT(services.attributes__field_solution_detail__processed, 279) || '…'
        END                                                          AS "presentation_resume",
        NULL                                                         AS "prise_rdv",
        NULL                                                         AS "recurrence",
        services._di_source_id                                       AS "source",
        structures.id                                                AS "structure_id",
        CASE
            WHEN services.attributes__field_lien_aide_uri IS NULL THEN structures.telephone
        END                                                          AS "telephone",
        code_insee.code_region                                       AS "zone_diffusion_code",
        code_insee.nom_region                                        AS "zone_diffusion_nom",
        'region'                                                     AS "zone_diffusion_type",
        structures.id || '-' || services.id                          AS "id",
        NULL                                                         AS "page_web",
        CAST(NULL AS TEXT [])                                        AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                                        AS "mobilisable_par",
        NULL                                                         AS "mobilisation_precisions",
        NULL                                                         AS "modes_orientation_accompagnateur_autres",
        NULL                                                         AS "modes_orientation_beneficiaire_autres",
        CAST(NULL AS TEXT [])                                        AS "justificatifs",
        CAST(NULL AS TEXT [])                                        AS "pre_requis",
        'https://www.agefiph.fr' || services.attributes__path__alias AS "lien_source",
        CAST(services.attributes__changed AS DATE)                   AS "date_maj",
        CASE
            WHEN services.attributes__field_montant_aide IS NULL THEN services.attributes__field_solution_detail__processed
            ELSE services.attributes__field_solution_detail__processed || '\nAide d’un montant de ' || services.attributes__field_montant_aide
        END                                                          AS "presentation_detail",
        -- WIP (cf https://www.notion.so/gip-inclusion/24610bd08f8a412c83c09f6b36a1a44f?v=34cdd4c049e44f49aec060657c72c9b0&p=1d75f321b60480d9aca0d55129ea310e&pm=s)
        ARRAY(
            SELECT di_thematique_by_agefiph_thematique.thematique
            FROM services_thematiques
            INNER JOIN di_thematique_by_agefiph_thematique ON services_thematiques.thematique_id = di_thematique_by_agefiph_thematique.agefiph_thematique_id
            WHERE services.id = services_thematiques.service_id
        )                                                            AS "thematiques",
        ARRAY['a-distance']                                          AS "modes_accueil",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NOT NULL THEN 'completer-le-formulaire-dadhesion' END
            ],
            NULL
        )                                                            AS "modes_orientation_accompagnateur",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NOT NULL THEN 'completer-le-formulaire-dadhesion' END
            ],
            NULL
        )                                                            AS "modes_orientation_beneficiaire",
        (
            SELECT di_profils_by_agefiph_profil.profil
            FROM di_profils_by_agefiph_profil
            WHERE services.relationships__field_profil_associe__data__id = di_profils_by_agefiph_profil.agefiph_profil
        )                                                            AS "profils",
        (
            SELECT di_profils_by_agefiph_profil.profil_precision
            FROM di_profils_by_agefiph_profil
            WHERE services.relationships__field_profil_associe__data__id = di_profils_by_agefiph_profil.agefiph_profil
        )                                                            AS "profils_precisions",
        ARRAY(
            SELECT di_type_by_agefiph_type.type_
            FROM di_type_by_agefiph_type
            WHERE services.relationships__field_type_de_solution__data__id = di_type_by_agefiph_type.agefiph_type
        )                                                            AS "types",
        CAST(NULL AS FLOAT)                                          AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                            AS "nombre_semaines",
        ARRAY['gratuit']                                             AS "frais"
    FROM
        structures
    CROSS JOIN services
    INNER JOIN structures_staging ON structures.id = structures_staging.id
    LEFT JOIN code_insee
        ON
            structures_staging.attributes__field_adresse__locality = code_insee.input_commune
            AND structures_staging.attributes__field_adresse__postal_code = code_insee.input_code_postal
            AND code_insee.distance < 0.6
    INNER JOIN di_type_by_agefiph_type AS aget
        ON services.relationships__field_type_de_solution__data__id = aget.agefiph_type
    INNER JOIN di_profils_by_agefiph_profil AS agep
        ON services.relationships__field_profil_associe__data__id = agep.agefiph_profil
    WHERE services.attributes__field_solution_partenaire = 'false'
)

SELECT * FROM final
