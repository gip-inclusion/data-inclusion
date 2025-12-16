{{ config(materialized='ephemeral') }}

SELECT
    x.formacode_v14,
    x.thematique
FROM (
    VALUES
    (
        '11083', -- mathématiques mise à niveau
        'lecture-ecriture-calcul--maitriser-le-calcul'
    ),
    (
        '15022', -- lutte contre l'illettrisme
        'lecture-ecriture-calcul--maitriser-le-francais'
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
        'numerique--maitriser-les-fondamentaux-du-numerique'
    ),
    (
        '15061', -- accompagnement vers emploi
        'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'
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
