WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

fredo_types AS (SELECT * FROM {{ ref('stg_fredo__types') }}),

di_typologie_by_fredo_type_structure AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://www.notion.so/dora-beta/8c8d883758df47649902ddba55ab2236?v=56c7542b590c42febc79db8d3ffe9f81&p=84e6e41b6a3b4f54bee46e11aa41ff47&pm=s
        ('ASSO', 'Association'),
        ('REG', 'Collectivité Territoriale de la Région'),
        ('MUNI', 'Collectivité Territoriale de la Ville'),
        ('CD', 'Collectivité Territoriale du Département'),
        ('ETABL PRI', 'Entreprise'),
        ('ETABL PRI', 'Etablissement privé'),
        ('ETABL PUB', 'Etablissement Public'),
        ('ETAT', 'Services de l''Etat')
    ) AS x (typologie, type_structure)
),

final AS (
    SELECT
        id                             AS "id",
        id                             AS "adresse_id",
        _di_source_id                  AS "source",
        siret                          AS "siret",
        NULL                           AS "rna",
        nom                            AS "nom",
        last_update                    AS "date_maj",
        (
            SELECT di_typologie_by_fredo_type_structure.typologie
            FROM di_typologie_by_fredo_type_structure
            INNER JOIN fredo_types AS stg_type ON stg_type.structure_id = structures.id
            WHERE stg_type.value = di_typologie_by_fredo_type_structure.type_structure
            LIMIT 1
        )                              AS "typologie",
        telephone                      AS "telephone",
        NULL                           AS "courriel",
        NULL                           AS "site_web",
        LEFT(presentation_resume, 280) AS "presentation_resume",
        CASE
            WHEN CHAR_LENGTH(presentation_resume) > 280
                THEN presentation_resume
        END                            AS "presentation_detail",
        CAST(NULL AS BOOLEAN)          AS "antenne",
        lien_source                    AS "lien_source",
        horaires_ouverture             AS "horaires_ouverture",
        NULL                           AS "accessibilite",
        CAST(NULL AS TEXT [])          AS "labels_nationaux",
        CAST(NULL AS TEXT [])          AS "labels_autres",
        CAST(NULL AS TEXT [])          AS "thematiques"
    FROM structures
)

SELECT * FROM final
