WITH structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
)

SELECT
    NULL                                                                                         AS "accessibilite",
    structures.id                                                                                AS "adresse_id",
    structures.attributes__field_courriel                                                        AS "courriel",
    structures.attributes__field_texte_brut_long                                                 AS "horaires_ouverture",
    structures.id                                                                                AS "id",
    NULL                                                                                         AS "lien_source",
    structures.attributes__title                                                                 AS "nom",
    NULL                                                                                         AS "rna",
    NULL                                                                                         AS "siret",
    structures.attributes__field_lien_externe__uri                                               AS "site_web",
    'agefiph'                                                                                    AS "source",
    structures.attributes__field_telephone                                                       AS "telephone",
    'ASSO'                                                                                       AS "typologie",
    'L’Agefiph est chargée de soutenir le développement de l’emploi des personnes handicapées. ' AS "presentation_resume",
    CAST(structures.attributes__changed AS DATE)                                                 AS "date_maj",
    (
        'L’Agefiph aide les personnes à se former, trouver un emploi ou créer leur entreprise ; '
        'elle permet à toute personne handicapée de compenser les conséquences de son handicap en '
        'situation d’emploi ou de formation. L’Agefiph propose des solutions ciblées et personnalisées '
        'sous forme d’aides financières, services et accompagnement.'
    )                                                                                            AS "presentation_detail",
    CAST(NULL AS TEXT [])                                                                        AS "labels_autres",
    ARRAY['agefiph']                                                                             AS "labels_nationaux",
    ARRAY['handicap']                                                                            AS "thematiques"
FROM structures
