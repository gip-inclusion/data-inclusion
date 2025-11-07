{{ config(materialized='ephemeral') }}

SELECT
    x.thematique_id,
    x.thematique_name
FROM (
    VALUES
    ('51be0003-13d8-4ffa-9923-248e7aa4a227', 'Compenser le handicap / Aménager le poste'),
    ('577b4a6a-86d5-46b7-a309-2f1935871cb6', 'Recruter'),
    ('5f6e19f8-a7fc-4bba-8c8a-c742bd70b3ed', 'Maintenir dans l''emploi'),
    ('644da4e8-c7f7-42ca-85e8-7ec8f23f06b9', 'Être accompagné'),
    ('6dc2c60c-40e2-4b72-a21e-7f4166cdb03f', 'Se former / Développer les compétences'),
    ('76c39fa6-c33f-4bb1-8751-74c66f67e4cf', 'Financer un projet'),
    ('84555d8c-75cd-447f-a89d-92d6942f8c10', 'S''informer'),
    ('8c43e73e-e41f-4e17-922a-84b8cae5c48f', 'Sensibiliser / Informer les équipes'),
    ('990df11c-db08-4ad5-ba7a-e9f7c4b8e66d', 'Devenir employeur / Gérer l''administratif'),
    ('a3893ddb-07b1-4ccb-a44b-0d6c69d99c44', 'Être accompagné(e)'),
    ('b28d9cf5-abdc-4ae0-abb5-de76f85e0e3c', 'Trouver un emploi / Chercher un stage'),
    ('b81ead52-da09-48b3-9fdc-e75d6edf5cce', 'Se préparer à un recrutement / Préparer sa candidature'),
    ('bf2c18ec-f21d-404e-a2fe-d2a5f0f3bbd5', 'Maintenir en emploi / Reclasser'),
    ('c5cd0f8f-7d39-4e73-8cb8-7e1ca3b70e95', 'Créer ou reprendre une entreprise'),
    ('d98ddd83-cb3d-4857-8b75-4b6af2d50fc1', 'S''informer'),
    ('db7a15ae-2f6a-4f8e-b84e-3b7cfd4ffddf', 'Travailler à son compte / Développer son activité'),
    ('ddf0fa87-2ee0-481c-a258-96985b7826c3', 'Créer ou reprendre une activité')
) AS x (thematique_id, thematique_name)
