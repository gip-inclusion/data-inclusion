{{ config(materialized='ephemeral') }}

SELECT
    x.dispositif,
    x.reseau_porteur
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Aidants Connect',                      'aidants-connect'),
    ('Conseillers numériques',               'conseillers-numeriques'),
    ('France Services',                      'france-service'),
    ('Grande école du numérique',            'grandes-ecoles-du-numerique'),
    ('La Croix Rouge',                       'croix-rouge'),
    ('Bibliothèques numérique de référence', NULL),
    ('Certification PIX',                    NULL),
    ('Point d''accès numérique CAF',         NULL),
    ('Promeneurs du net',                    NULL)
    -- noqa: enable=layout.spacing
) AS x (dispositif, reseau_porteur)
WHERE x.reseau_porteur IS NOT NULL
