{{ config(materialized='ephemeral') }}
SELECT
    x.type,
    x.mode_orientation,
    UNNEST(x.modes_mobilisation) AS mode_mobilisation
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('accompagnateur', 'autre',                                          NULL),
    ('accompagnateur', 'completer-le-formulaire-dadhesion',              ARRAY['utiliser-lien-mobilisation']),
    ('accompagnateur', 'envoyer-un-mail',                                ARRAY['envoyer-un-courriel']),
    ('accompagnateur', 'envoyer-un-mail-avec-une-fiche-de-prescription', ARRAY['envoyer-un-courriel']),
    ('accompagnateur', 'prendre-rdv',                                    ARRAY['envoyer-un-courriel', 'utiliser-lien-mobilisation']),
    ('accompagnateur', 'telephoner',                                     ARRAY['telephoner']),
    ('beneficiaire',   'autre',                                          NULL),
    ('beneficiaire',   'completer-le-formulaire-dadhesion',              ARRAY['utiliser-lien-mobilisation']),
    ('beneficiaire',   'envoyer-un-mail',                                ARRAY['envoyer-un-courriel']),
    ('beneficiaire',   'prendre-rdv',                                    ARRAY['envoyer-un-courriel', 'utiliser-lien-mobilisation']),
    ('beneficiaire',   'se-presenter',                                   ARRAY['se-presenter']),
    ('beneficiaire',   'telephoner',                                     ARRAY['telephoner'])
    -- noqa: enable=layout.spacing
) AS x (type, mode_orientation, modes_mobilisation)
WHERE x.modes_mobilisation IS NOT NULL
