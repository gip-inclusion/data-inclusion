{{ config(materialized='ephemeral') }}

-- https://www.agefiph.fr/jsonapi/taxonomy_term/thematique
-- only kept the ones with `field_page_besoin = true`
SELECT
    x.agefiph_thematique_id,
    x.thematique
FROM (
    VALUES
    ('03228d62-2a59-49d8-8443-b25cb2e684b9', 'creer-une-entreprise--developper-son-entreprise'),
    ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'choisir-un-metier--decouvrir-un-metier-ou-un-secteur-dactivite'),
    ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'choisir-un-metier--identifier-ses-points-forts-et-ses-competences'),
    ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'trouver-un-emploi--maintien-dans-lemploi'),
    ('5d8c88d8-db03-4f27-b517-d7016896b01a', 'creer-une-entreprise--developper-son-entreprise'),
    ('78b28acb-803e-4b06-ab77-58dabfbd8571', 'creer-une-entreprise--definir-son-projet-de-creation-dentreprise'),
    ('78b28acb-803e-4b06-ab77-58dabfbd8571', 'creer-une-entreprise--structurer-son-projet-de-creation-dentreprise'),
    ('907a8c33-5c56-49d3-bd64-a736a9ceac76', 'creer-une-entreprise--developper-son-entreprise'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'equipement-et-alimentation--aide-menagere'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'mobilite--financer-ma-mobilite'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'numerique--acquerir-un-equipement'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'sante--acces-aux-soins'),
    ('cb2c9fec-c190-4e2f-aeee-6da818109bf8', 'choisir-un-metier--confirmer-son-choix-de-metier'),
    ('cb2c9fec-c190-4e2f-aeee-6da818109bf8', 'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'),
    ('cb2c9fec-c190-4e2f-aeee-6da818109bf8', 'trouver-un-emploi--maintien-dans-lemploi'),
    ('ddf0fa87-2ee0-481c-a258-96985b7826c3', 'trouver-un-emploi--maintien-dans-lemploi'),
    ('f4551558-8315-4708-8357-5ecc89751bc6', 'preparer-sa-candidature--realiser-un-cv-et-ou-une-lettre-de-motivation'),
    ('f4551558-8315-4708-8357-5ecc89751bc6', 'trouver-un-emploi--repondre-a-des-offres-demploi'),
    ('f9ab3e06-af51-463a-aaf7-7b04a28e047f', 'creer-une-entreprise--developper-son-entreprise'),
    ('f9ab3e06-af51-463a-aaf7-7b04a28e047f', 'se-former--monter-son-dossier-de-formation')
) AS x (agefiph_thematique_id, thematique)
