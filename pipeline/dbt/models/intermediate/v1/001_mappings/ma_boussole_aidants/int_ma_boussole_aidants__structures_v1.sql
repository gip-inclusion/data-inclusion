WITH structures AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures') }}
),

final AS (
    SELECT
        'ma-boussole-aidants'                   AS "source",
        'ma-boussole-aidants--' || id_structure AS "id",
        'ma-boussole-aidants--' || id_structure AS "adresse_id",
        nom_structure                           AS "nom",
        last_modified_date                      AS "date_maj",
        CASE
            WHEN departement__code_departement IS NOT NULL
                THEN
                    FORMAT(
                        'https://maboussoleaidants.fr/mes-solutions/%s/%s/%s',
                        CASE
                            WHEN id_type_structure = '15' THEN 'centre-communal-action-sociale-ccas'
                            WHEN id_type_structure = '53' THEN 'maison-departementale-autonomie-mda'
                            WHEN id_type_structure = '127' THEN 'maison-departementale-des-solidarites'
                        END,
                        departement__code_departement,
                        id_structure
                    )
        END                                     AS "lien_source",
        structure_mere__siret                   AS "siret",
        telephone_1                             AS "telephone",
        email                                   AS "courriel",
        site_web                                AS "site_web",
        description_structure                   AS "description",
        NULL                                    AS "horaires_accueil",
        NULL                                    AS "accessibilite_lieu",
        CASE
            WHEN id_type_structure = '15' THEN 'ccas-cias'
            WHEN id_type_structure = '53' THEN 'maison-departementale-de-lautonomie'
            WHEN id_type_structure = '127' THEN 'maisons-des-solidarites'
        END                                     AS "reseaux_porteurs"
    FROM structures
)

SELECT * FROM final
