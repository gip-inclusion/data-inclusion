{{ config(materialized='ephemeral') }}

-- https://www.agefiph.fr/jsonapi/taxonomy_term/thematique
SELECT
    x.agefiph_thematique_id,
    x.thematique
FROM (
    VALUES
    ('4e08047f-b0ed-431a-9182-61e8e61b1486', 'se-former--trouver-sa-formation'),
    ('907a8c33-5c56-49d3-bd64-a736a9ceac76', 'creer-une-entreprise--developper-son-entreprise'),
    ('60c25ci7-61sc-89a9-ny54-126hslf808a2', 'trouver-un-emploi--maintien-dans-lemploi'),
    ('cb2c9fec-c190-4e2f-aeee-6da818109bf8', 'choisir-un-metier--confirmer-son-choix-de-metier'),
    ('78b28acb-803e-4b06-ab77-58dabfbd8571', 'creer-une-entreprise--definir-son-projet-de-creation-dentreprise'),
    ('fb5e6180-290b-4216-ba68-624d25defa3a', 'creer-une-entreprise--developper-son-entreprise'),
    ('03228d62-2a59-49d8-8443-b25cb2e684b9', 'creer-une-entreprise--developper-son-entreprise'),
    ('f9ab3e06-af51-463a-aaf7-7b04a28e047f', 'se-former--trouver-sa-formation'),
    ('aeab1d68-4e89-4e2a-a612-d8645e3999d8', 'creer-une-entreprise--developper-son-entreprise'),
    ('f4551558-8315-4708-8357-5ecc89751bc6', 'trouver-un-emploi--repondre-a-des-offres-demploi'),
    ('f4551558-8315-4708-8357-5ecc89751bc6', 'trouver-un-emploi--faire-des-candidatures-spontanees'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'se-former--trouver-sa-formation'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'trouver-un-emploi--maintien-dans-lemploi'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'creer-une-entreprise--definir-son-projet-de-creation-dentreprise'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'trouver-un-emploi--repondre-a-des-offres-demploi'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'trouver-un-emploi--faire-des-candidatures-spontanees'),
    ('5d8c88d8-db03-4f27-b517-d7016896b01a', 'creer-une-entreprise--developper-son-entreprise'),
    ('5d8c88d8-db03-4f27-b517-d7016896b01a', 'trouver-un-emploi--maintien-dans-lemploi'),
    ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'se-former--trouver-sa-formation'),
    ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'se-former--monter-son-dossier-de-formation'),
    ('ddf0fa87-2ee0-481c-a258-96985b7826c3', 'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits'),
    ('ddf0fa87-2ee0-481c-a258-96985b7826c3', 'trouver-un-emploi--maintien-dans-lemploi')
) AS x (agefiph_thematique_id, thematique)
