WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

fredo_categories AS (SELECT * FROM {{ ref('stg_fredo__categories') }}),

fredo_types AS (SELECT * FROM {{ ref('stg_fredo__types') }}),

map_thematiques AS (SELECT * FROM {{ ref('_map_fredo_thematiques') }}),

thematiques AS (
    SELECT
        fredo_categories.structure_id,
        ARRAY_AGG(DISTINCT map_thematiques.thematiques) AS thematiques
    FROM fredo_categories
    INNER JOIN map_thematiques ON fredo_categories.value = map_thematiques.category
    GROUP BY fredo_categories.structure_id
),

di_typologie_by_fredo_type_structure AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://www.notion.so/dora-beta/8c8d883758df47649902ddba55ab2236?v=56c7542b590c42febc79db8d3ffe9f81&p=84e6e41b6a3b4f54bee46e11aa41ff47&pm=s
        ('ASSO', 'association'),
        ('REG', 'collectivité territoriale de la région'),
        ('MUNI', 'collectivité territoriale de la ville'),
        ('CD', 'collectivité territoriale du département'),
        ('ETABL_PRI', 'entreprise'),
        ('ETABL_PRI', 'etablissement privé'),
        ('ETABL_PUB', 'etablissement public'),
        ('ETAT', 'services de l''etat')
    ) AS x (typologie, type_structure)
),

final AS (
    SELECT
        structures.id                             AS "id",
        structures.id                             AS "adresse_id",
        'fredo'                                   AS "source",
        structures.siret                          AS "siret",
        NULL                                      AS "rna",
        structures.nom                            AS "nom",
        structures.last_update                    AS "date_maj",
        (
            SELECT di_typologie_by_fredo_type_structure.typologie
            FROM di_typologie_by_fredo_type_structure
            INNER JOIN fredo_types AS stg_type ON stg_type.structure_id = structures.id
            WHERE stg_type.value = di_typologie_by_fredo_type_structure.type_structure
            LIMIT 1
        )                                         AS "typologie",
        CASE
            WHEN ARRAY_LENGTH(structures.telephone, 1) > 0 THEN structures.telephone[1]
        END                                       AS "telephone",
        CASE
            WHEN ARRAY_LENGTH(structures.courriel, 1) > 0 THEN structures.courriel[1]
        END                                       AS "courriel",
        structures.site_web                       AS "site_web",
        LEFT(structures.presentation_resume, 280) AS "presentation_resume",
        structures.presentation_resume            AS "presentation_detail",
        NULL                                      AS "lien_source",
        structures.horaires_ouverture             AS "horaires_ouverture",
        NULL                                      AS "accessibilite",
        CAST(NULL AS TEXT [])                     AS "labels_nationaux",
        CAST(NULL AS TEXT [])                     AS "labels_autres",
        thematiques.thematiques                   AS "thematiques"
    FROM structures
    LEFT JOIN thematiques ON structures.id = thematiques.structure_id
)

SELECT * FROM final
