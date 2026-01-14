{{ config(materialized='ephemeral') }}

SELECT
    x.service,
    UNNEST(x.thematiques) AS "thematique"
FROM (
    VALUES
    (
        'Accès internet et matériel informatique',
        ARRAY[
            'numerique--acceder-a-une-connexion-internet',
            'numerique--acquerir-un-equipement'
        ]
    ),
    (
        'Acquisition de matériel informatique à prix solidaire',
        ARRAY[
            'numerique--acquerir-un-equipement'
        ]
    ),
    (
        'Aide aux démarches administratives',
        ARRAY[
            'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives',
            'numerique--acceder-a-des-services-en-ligne'
        ]
    ),
    (
        'Compréhension du monde numérique',
        ARRAY[
            'numerique--maitriser-les-fondamentaux-du-numerique'
        ]
    ),
    (
        'Insertion professionnelle via le numérique',
        ARRAY[
            'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi',
            'numerique--maitriser-les-fondamentaux-du-numerique'
        ]
    ),
    (
        'Loisirs et créations numériques',
        ARRAY[
            'remobilisation--activites-sportives-et-culturelles',
            'numerique--maitriser-les-fondamentaux-du-numerique'
        ]
    ),
    (
        'Maîtrise des outils numériques du quotidien',
        ARRAY[
            'numerique--maitriser-les-fondamentaux-du-numerique',
            'numerique--acceder-a-des-services-en-ligne'
        ]
    ),
    (
        'Parentalité et éducation avec le numérique',
        ARRAY[
            'famille--soutien-a-la-parentalite-et-a-leducation',
            'numerique--maitriser-les-fondamentaux-du-numerique'
        ]
    ),
    (
        'Utilisation sécurisée du numérique',
        ARRAY[
            'numerique--maitriser-les-fondamentaux-du-numerique'
        ]
    )
) AS x (service, thematiques)
WHERE x.thematiques IS NOT NULL
