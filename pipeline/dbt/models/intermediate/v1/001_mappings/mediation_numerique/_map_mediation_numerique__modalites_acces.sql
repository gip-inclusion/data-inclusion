{{ config(materialized='ephemeral') }}

SELECT
    x.modalite_source,
    x.mode_mobilisation AS "mode_mobilisation"
FROM (
    VALUES
    ('Contacter par mail', 'envoyer-un-courriel'),
    ('Envoyer un mail avec une fiche de prescription', 'envoyer-un-courriel'),
    ('Prendre un RDV en ligne', 'utiliser-lien-mobilisation'),
    ('Se présenter', 'se-presenter'),
    ('Téléphoner', 'telephoner')
) AS x (modalite_source, mode_mobilisation)
