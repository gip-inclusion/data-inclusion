WITH structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
)

SELECT
    'agefiph'                                                                                   AS "source",
    'agefiph--' || structures.id                                                                AS "id",
    'agefiph--' || structures.id                                                                AS "adresse_id",
    structures.attributes__title                                                                AS "nom",
    (
        'L’Agefiph aide les personnes à se former, trouver un emploi ou créer leur entreprise ; '
        'elle permet à toute personne handicapée de compenser les conséquences de son handicap en '
        'situation d’emploi ou de formation. L’Agefiph propose des solutions ciblées et personnalisées '
        'sous forme d’aides financières, services et accompagnement.'
    )                                                                                           AS "description",
    COALESCE(structures.attributes__field_lien_externe__uri, 'https://www.agefiph.fr/annuaire') AS "lien_source",
    COALESCE(structures.attributes__field_lien_externe__uri, 'https://www.agefiph.fr/annuaire') AS "site_web",
    CAST(structures.attributes__changed AS DATE)                                                AS "date_maj",
    structures.attributes__field_courriel                                                       AS "courriel",
    structures.attributes__field_texte_brut_long                                                AS "horaires_accueil",
    NULL                                                                                        AS "siret",
    structures.attributes__field_telephone                                                      AS "telephone",
    ARRAY['agefiph']                                                                            AS "reseaux_porteurs",
    NULL                                                                                        AS "accessibilite_lieu"
FROM structures
