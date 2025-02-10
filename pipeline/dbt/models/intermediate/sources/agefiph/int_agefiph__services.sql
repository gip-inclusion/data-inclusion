WITH services_publics AS (
    SELECT * FROM {{ ref('stg_agefiph__services_publics') }}
),

services AS (
    SELECT
        s.*,
        sp.public_id
    FROM {{ ref('stg_agefiph__services') }} AS s
    INNER JOIN services_publics AS sp
        ON s.id = sp.service_id
    -- public : "Personne handicapée"
    -- https://www.agefiph.fr/jsonapi/taxonomy_term/public_cible/0d0b63b6-4043-4b2d-a3f6-d7c85f335070
    WHERE sp.public_id = '0d0b63b6-4043-4b2d-a3f6-d7c85f335070'
    -- services are managed by CAP Emploi, their information is in DORA, linked to different structures
    AND s.id NOT IN ('8c6e19c3-db7e-40b4-a3de-36d49b9efc75', '3d026af1-f5d7-4b25-9876-2d6506984eb1')
),

services_thematiques AS (
    SELECT * FROM {{ ref('stg_agefiph__services_thematiques') }}
),

structures AS (
    SELECT * FROM {{ ref('int_agefiph__structures') }}
),

-- https://www.agefiph.fr/jsonapi/taxonomy_term/thematique
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
        ('f4551558-8315-4708-8357-5ecc89751bc6', 'handicap--faire-reconnaitre-un-handicap'),
        ('5d8c88d8-db03-4f27-b517-d7016896b01a', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi'),
        ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'handicap--connaissance-des-droits-des-travailleurs'),
        ('ddf0fa87-2ee0-481c-a258-96985b7826c3', 'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi')
    ) AS x (agefiph_thematique_id, thematique)
),

-- https://www.agefiph.fr/jsonapi/taxonomy_term/type_aide_service
di_type_by_agefiph_type AS (
    SELECT x.*
    FROM (
        VALUES
        ('9f1b3cad-7a62-449a-8ac2-3356c939f827', 'aide-financiere'),
        ('6a94011c-a840-4ad5-824f-81738e8a1821', 'accompagnement'),
        ('0f8de1a1-1b4b-4508-b235-ada518a806e4', 'accompagnement')
    ) AS x (agefiph_type, type_)
),

final AS (
    SELECT
        structures.id                                                AS "adresse_id",
        TRUE                                                         AS "contact_public",
        NULL                                                         AS "contact_nom_prenom",
        structures.courriel                                          AS "courriel",
        NULL                                                         AS "formulaire_en_ligne",
        NULL                                                         AS "frais_autres",
        services.attributes__title                                   AS "nom",
        services.attributes__field_titre_card_employeur              AS "presentation_resume",
        NULL                                                         AS "prise_rdv",
        NULL                                                         AS "recurrence",
        services._di_source_id                                       AS "source",
        structures.id                                                AS "structure_id",
        structures.telephone                                         AS "telephone",
        NULL                                                         AS "zone_diffusion_code",
        NULL                                                         AS "zone_diffusion_nom",
        'pays'                                                       AS "zone_diffusion_type",
        services.id                                                  AS "id",
        NULL                                                         AS "page_web",
        'https://www.agefiph.fr' || services.attributes__path__alias AS "modes_orientation_accompagnateur_autres",
        'https://www.agefiph.fr' || services.attributes__path__alias AS "modes_orientation_beneficiaire_autres",
        CAST(NULL AS TEXT [])                                        AS "justificatifs",
        CAST(NULL AS TEXT [])                                        AS "pre_requis",
        CAST(NULL AS BOOLEAN)                                        AS "cumulable",
        CAST(NULL AS DATE)                                           AS "date_suspension",
        'https://www.agefiph.fr' || services.attributes__path__alias AS "lien_source",
        CAST(services.attributes__created AS DATE)                   AS "date_creation",
        CAST(services.attributes__changed AS DATE)                   AS "date_maj",
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
        )                                                            AS "presentation_detail",
        ARRAY(
            SELECT di_thematique_by_agefiph_thematique.thematique
            FROM services_thematiques
            INNER JOIN di_thematique_by_agefiph_thematique ON services_thematiques.thematique_id = di_thematique_by_agefiph_thematique.agefiph_thematique_id
            WHERE services.id = services_thematiques.service_id
        )                                                            AS "thematiques",
        ARRAY['a-distance']                                          AS "modes_accueil",
        ARRAY['autre']                                               AS "modes_orientation_accompagnateur",
        ARRAY['autre']                                               AS "modes_orientation_beneficiaire",
        ARRAY[
            'personnes-en-situation-de-handicap'
        ]                                                            AS "profils",
        NULL                                                         AS "profils_precisions",
        ARRAY(
            SELECT di_type_by_agefiph_type.type_
            FROM di_type_by_agefiph_type
            WHERE services.relationships__field_type_aide_service__data__id = di_type_by_agefiph_type.agefiph_type
        )                                                            AS "types",
        CAST(NULL AS TEXT [])                                        AS "frais"
    FROM
        structures
    CROSS JOIN services
)

SELECT * FROM final
