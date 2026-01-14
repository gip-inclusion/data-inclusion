{{ config(materialized='ephemeral') }}

SELECT
    x.modalite,
    x.mode_mobilisation
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Contacter par mail',                             'envoyer-un-courriel'),
    ('Envoyer un mail avec une fiche de prescription', 'envoyer-un-courriel'),
    ('Prendre un RDV en ligne',                        'utiliser-lien-mobilisation'),
    ('Se présenter',                                   'se-presenter'),
    ('Téléphoner',                                     'telephoner')
    -- noqa: enable=layout.spacing
) AS x (modalite, mode_mobilisation)
WHERE x.mode_mobilisation IS NOT NULL
