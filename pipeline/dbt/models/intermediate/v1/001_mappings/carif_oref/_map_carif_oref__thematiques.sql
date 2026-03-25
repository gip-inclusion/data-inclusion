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
        'lecture-ecriture-calcul--maitriser-le-calcul'
    ),
    (
        '15041', -- mise à niveau
        'lecture-ecriture-calcul--maitriser-le-francais'
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
        '15061', -- accompagnement vers emploi
        'trouver-un-emploi--suivre-ses-candidatures-et-relancer-les-employeurs'
    ),
    (
        '15061', -- accompagnement vers emploi
        'choisir-un-metier--confirmer-son-choix-de-metier'
    ),
    (
        '15062', -- bilan de compétences et projet professionnel
        'choisir-un-metier--identifier-ses-points-forts-et-ses-competences'
    ),
    (
        '15062', -- bilan de compétences et projet professionnel
        'choisir-un-metier--confirmer-son-choix-de-metier'
    ),
    (
        '15066', -- bien-être et confiance en soi
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
    ),
    (
        '15436', -- activités sportives et culturelles
        'remobilisation--activites-sportives-et-culturelles'
    ),
    (
        '32154', -- identification des compétences
        'choisir-un-metier--identifier-ses-points-forts-et-ses-competences'
    ),
    (
        '44028', -- prise en charge personne dépendante
        'famille--prise-en-charge-personne-dependante'
    )
) AS x (formacode_v14, thematique)
