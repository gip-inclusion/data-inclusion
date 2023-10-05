WITH structures AS (
    SELECT * FROM {{ ref('stg_immersion_facilitee__structures') }}
),

di_structure_typologie_by_immersion_facilitee_kind AS (
    SELECT x.*
    FROM (
        VALUES
        ('structure-IAE', 'EI'),
        ('mission-locale', 'ML'),
        ('cci', 'CCONS'),
        ('cap-emploi', 'CAP_EMPLOI'),
        ('pole-emploi', 'PE'),
        ('conseil-departemental', 'CD')
    -- 'immersion-facile' -> ?
    -- 'prepa-apprentissage' -> ?
    -- 'autre' -> NULL
    ) AS x (kind, typologie)
),

final AS (
    SELECT
        id                    AS "id",
        id                    AS "adresse_id",
        NULL::BOOLEAN         AS "antenne",
        NULL                  AS "rna",
        'immersion-facilitee' AS "source",
        NULL                  AS "horaires_ouverture",
        NULL                  AS "accessibilite",
        NULL::TEXT []         AS "labels_nationaux",
        NULL::TEXT []         AS "labels_autres",
        NULL::TEXT []         AS "thematiques",
        created_at            AS "date_maj",
        "agency_siret"        AS "siret",
        "name"                AS "nom",
        NULL                  AS "lien_source",
        NULL                  AS "presentation_resume",
        NULL                  AS "presentation_detail",
        NULL                  AS "telephone",
        NULL                  AS "site_web",
        NULL                  AS "courriel",
        (
            SELECT di_structure_typologie_by_immersion_facilitee_kind.typologie
            FROM di_structure_typologie_by_immersion_facilitee_kind
            WHERE structures.kind = di_structure_typologie_by_immersion_facilitee_kind.kind
        )                     AS "typologie"
    FROM structures
)

SELECT * FROM final
