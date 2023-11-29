SELECT
    NULL                                                                                         AS "accessibilite",
    'c7f4be8b-309e-4a6a-b562-c4f7f3bb3c5c'                                                       AS "adresse_id",
    FALSE                                                                                        AS "antenne",
    NULL                                                                                         AS "courriel",
    NULL                                                                                         AS "horaires_ouverture",
    'c7f4be8b-309e-4a6a-b562-c4f7f3bb3c5c'                                                       AS "id",
    NULL                                                                                         AS "lien_source",
    'Agefiph'                                                                                    AS "nom",
    'W921000912'                                                                                 AS "rna",
    '34995887600188'                                                                             AS "siret",
    'https://www.agefiph.fr/'                                                                    AS "site_web",
    'agefiph'                                                                                    AS "source",
    '0800 111 009'                                                                               AS "telephone",
    'ASSO'                                                                                       AS "typologie",
    'L’Agefiph est chargée de soutenir le développement de l’emploi des personnes handicapées. ' AS "presentation_resume",
    CAST('2023-12-01' AS DATE)                                                                   AS "date_maj",
    (
        'L’Agefiph aide les personnes à se former, trouver un emploi ou créer leur entreprise ; '
        'elle permet à toute personne handicapée de compenser les conséquences de son handicap en '
        'situation d’emploi ou de formation. L’Agefiph propose des solutions ciblées et personnalisées '
        'sous forme d’aides financières, services et accompagnement.'
    )                                                                                            AS "presentation_detail",
    CAST(NULL AS TEXT [])                                                                        AS "labels_autres",
    ARRAY['agefiph']                                                                             AS "labels_nationaux",
    ARRAY['handicap']                                                                            AS "thematiques"
