{{ config(materialized='ephemeral') }}

-- https://www.agefiph.fr/jsonapi/taxonomy_term/thematique

SELECT x.*
FROM (
    VALUES
    ('03228d62-2a59-49d8-8443-b25cb2e684b9', 'Connaître et respecter ses obligations emploi handicap'),
    ('366eb399-1e6c-4609-8066-d1504fae2a8e', 'Faire reconnaître le handicap'),
    ('4b8b0473-52c2-4a21-956d-d7d68a7053b5', 'Me former avec un handicap'),
    ('4e08047f-b0ed-431a-9182-61e8e61b1486', 'Développer l''accessibilité de vos formations'),
    ('51be0003-13d8-4ffa-9923-248e7aa4a227', 'Compenser le handicap / Aménager le poste'),
    ('5d8c88d8-db03-4f27-b517-d7016896b01a', 'Prendre en compte le handicap d''une personne à son poste'),
    ('60c25ci7-61sc-89a9-ny54-126hslf808a2', 'Handicap et emploi'),
    ('70bb7d35-ce81-46cc-b17b-01155a70c05c', 'Se former et sensibiliser ses collaborateurs au handicap'),
    ('78b28acb-803e-4b06-ab77-58dabfbd8571', 'Créer ou reprendre une entreprise'),
    ('907a8c33-5c56-49d3-bd64-a736a9ceac76', 'Développer et valoriser vos actions handicap'),
    ('9d609684-2597-4916-a897-753cfb0e8bc8', 'Compenser mon handicap'),
    ('aeab1d68-4e89-4e2a-a612-d8645e3999d8', 'Sous-traiter auprès du secteur protégé/adapté'),
    ('cb2c9fec-c190-4e2f-aeee-6da818109bf8', 'Construire mon projet professionnel'),
    ('ddf0fa87-2ee0-481c-a258-96985b7826c3', 'Travailler avec un handicap'),
    ('f4551558-8315-4708-8357-5ecc89751bc6', 'Rechercher un emploi'),
    ('f9ab3e06-af51-463a-aaf7-7b04a28e047f', 'Se former au handicap et sensibiliser ses collaborateurs au handicap'),
    ('fb5e6180-290b-4216-ba68-624d25defa3a', 'Recruter et intégrer un salarié en situation de handicap')
) AS x (id, name)
