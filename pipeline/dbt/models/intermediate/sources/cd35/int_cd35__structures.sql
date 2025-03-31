WITH organisations AS (
    SELECT * FROM {{ ref('stg_cd35__organisations') }}
),

final AS (
    SELECT
        id                                 AS "id",
        id                                 AS "adresse_id",
        NULL                               AS "siret",
        CAST(NULL AS BOOLEAN)              AS "antenne",
        NULL                               AS "rna",
        RTRIM(SUBSTRING(nom, 1, 150), '.') AS "nom",
        telephone                          AS "telephone",
        courriel                           AS "courriel",
        site_web                           AS "site_web",
        _di_source_id                      AS "source",
        lien_source                        AS "lien_source",
        horaires_ouvertures                AS "horaires_ouverture",
        NULL                               AS "accessibilite",
        CAST(NULL AS TEXT [])              AS "labels_nationaux",
        CAST(NULL AS TEXT [])              AS "labels_autres",
        CAST(NULL AS TEXT [])              AS "thematiques",
        date_maj                           AS "date_maj",
        CASE sigle
            WHEN 'CCAS' THEN 'CCAS'
            WHEN 'MAIRIE' THEN 'MUNI'
        END                                AS "typologie",
        CASE
            WHEN LENGTH(presentation_detail) <= 280 THEN presentation_detail
            ELSE LEFT(presentation_detail, 279) || '…'
        END                                AS "presentation_resume",
        CASE
            WHEN LENGTH(presentation_detail) <= 280 THEN NULL
            ELSE presentation_detail
        END                                AS "presentation_detail"
    FROM organisations
)

SELECT * FROM final
