{{ config(materialized='ephemeral') }}

SELECT
    x.formacode_v14,
    x.thematique
FROM (
    VALUES
    (
        '15022', -- lutte contre l'illettrisme
        'lecture-ecriture-calcul--maitriser-le-francais'
    ),
    (
        '11083', -- mathématiques mise à niveau
        'lecture-ecriture-calcul--maitriser-le-calcul'
    ),
    (
        '15030', -- calcul mise à niveau
        'lecture-ecriture-calcul--maitriser-le-calcul'
    ),
    (
        '15031', -- adaptation sociale
        'remobilisation--lien-social'
    ),
    (
        '15040', -- français mise à niveau
        'lecture-ecriture-calcul--maitriser-le-francais'
    ),
    (
        '15041', -- mise à niveau
        'remobilisation--activites-sportives-et-culturelles'
    ),
    (
        '15043', -- alphabétisation
        'lecture-ecriture-calcul--maitriser-le-francais'
    ),
    (
        '15050', -- mise à niveau numérique
        'lecture-ecriture-calcul--maitriser-le-calcul'
    ),
    (
        '15061', -- accompagnement vers emploi
        'remobilisation--bien-etre-confiance-en-soi'
    ),
    (
        '15084', -- préparation entrée formation
        'se-former--trouver-sa-formation'
    ),
    (
        '15235', -- français langue étrangère
        'lecture-ecriture-calcul--maitriser-le-francais'
    ),
    (
        '15281', -- français mise à niveau
        'lecture-ecriture-calcul--maitriser-le-francais'
    )
) AS x (formacode_v14, thematique)
