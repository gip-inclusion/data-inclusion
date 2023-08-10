WITH services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

services_thematiques AS (
    SELECT * FROM {{ ref('stg_agefiph__services_thematiques') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

communes AS (
    SELECT * FROM {{ source('insee', 'communes') }}
    WHERE "TYPECOM" != 'COMD'
),

regions AS (
    SELECT * FROM {{ source('insee', 'regions') }}
),

di_thematique_by_agefiph_thematique AS (
    SELECT x.*
    FROM (
        VALUES
        ('4e08047f-b0ed-431a-9182-61e8e61b1486', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('11618ce3-e59b-404f-8eb2-5763215464f2', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('60c25ci7-61sc-89a9-ny54-126hslf808a2', 'handicap--connaissance-des-droits-des-travailleurs'),
        ('cb2c9fec-c190-4e2f-aeee-6da818109bf8', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('78b28acb-803e-4b06-ab77-58dabfbd8571', 'handicap--adaptation-au-poste-de-travail'),
        ('fb5e6180-290b-4216-ba68-624d25defa3a', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('03228d62-2a59-49d8-8443-b25cb2e684b9', 'accompagnement-social-et-professionnel-personnalise--definition-du-projet-professionnel'),
        ('f9ab3e06-af51-463a-aaf7-7b04a28e047f', 'se-former--trouver-sa-formation'),
        ('aeab1d68-4e89-4e2a-a612-d8645e3999d8', 'creation-activite--definir-son-projet-de-creation-dentreprise'),
        ('f4551558-8315-4708-8357-5ecc89751bc6', 'handicap--faire-reconnaitre-un-handicap')
    ) AS x (agefiph_thematique_id, thematique)
),

final AS (
    SELECT
        structures.id                                                 AS "adresse_id",
        TRUE                                                          AS "contact_public",
        NULL                                                          AS "contact_nom_prenom",
        structures.courriel                                           AS "courriel",
        NULL                                                          AS "cumulable",
        NULL                                                          AS "date_suspension",
        NULL                                                          AS "formulaire_en_ligne",
        NULL                                                          AS "frais_autres",
        NULL                                                          AS "justificatifs",
        NULL                                                          AS "lien_source",
        services.attributes__title                                    AS "nom",
        services.attributes__field_titre_card_employeur               AS "presentation_resume",
        NULL                                                          AS "prise_rdv",
        NULL                                                          AS "recurrence",
        services._di_source_id                                        AS "source",
        structures.id                                                 AS "structure_id",
        structures.telephone                                          AS "telephone",
        regions."REG"                                                 AS "zone_diffusion_code",
        regions."LIBELLE"                                             AS "zone_diffusion_nom",
        'region'                                                      AS "zone_diffusion_type",
        NULL                                                          AS "pre_requis",
        CAST(services.attributes__created AS DATE)                    AS "date_creation",
        CAST(services.attributes__changed AS DATE)                    AS "date_maj",
        CAST(CAST(MD5(structures.id || services.id) AS UUID) AS TEXT) AS "id",
        NULLIF(
            TRIM(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN services.attributes__field_essentiel_ph__processed IS NOT NULL THEN 'Pour la personne handicapée :' || E'\n' || services.attributes__field_essentiel_ph__processed END,
                        CASE WHEN services.attributes__field_essentiel_employeur__processed IS NOT NULL THEN 'Pour l''employeur :' || E'\n' || services.attributes__field_essentiel_employeur__processed END,
                        services.attributes__field_texte_brut_long
                    ],
                    E'\n\n'
                )
            ),
            ''
        )                                                             AS "presentation_detail",
        ARRAY(
            SELECT di_thematique_by_agefiph_thematique.thematique
            FROM services_thematiques
            INNER JOIN di_thematique_by_agefiph_thematique ON services_thematiques.thematique_id = di_thematique_by_agefiph_thematique.agefiph_thematique_id
            WHERE services.id = services_thematiques.service_id
        )                                                             AS "thematiques",
        ARRAY['en-presentiel', 'a-distance']                          AS "modes_accueil",
        CAST(NULL AS TEXT [])                                         AS "modes_orientation_accompagnateur",
        CAST(NULL AS TEXT [])                                         AS "modes_orientation_beneficiaire",
        CAST(NULL AS TEXT [])                                         AS "profils",
        CAST(NULL AS TEXT [])                                         AS "types",
        CAST(NULL AS TEXT [])                                         AS "frais"
    FROM
        structures
    CROSS JOIN services
    LEFT JOIN communes ON structures.code_insee = communes."COM"
    LEFT JOIN regions ON communes."REG" = regions."REG"
)

SELECT * FROM final
