{{ config(materialized='ephemeral') }}

SELECT
    x.dispositif,
    x.reseau_porteur AS "reseau_porteur"
FROM (
    VALUES
    ('Aidants Connect', 'aidants-connect'),
    ('Conseillers numériques', 'conseillers-numeriques'),
    ('France Services', 'france-service'),
    ('Grande école du numérique', 'grandes-ecoles-du-numerique'),
    ('La Croix Rouge', 'croix-rouge'),
    ('Bibliothèques numérique de référence', NULL),
    ('Certification PIX', NULL),
    ('Point d''accès numérique CAF', NULL),
    ('Promeneurs du net', NULL)
) AS x (dispositif, reseau_porteur)
