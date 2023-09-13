WITH structures AS (
    SELECT * FROM {{ ref('stg_cd72__structures') }}
),

final AS (
    SELECT
        NULL                  AS "accessibilite",
        id                    AS "adresse_id",
        courriel              AS "courriel",
        date_maj              AS "date_maj",
        horaires_ouverture    AS "horaires_ouverture",
        id                    AS "id",
        NULL                  AS "lien_source",
        nom                   AS "nom",
        presentation_detail   AS "presentation_detail",
        NULL                  AS "presentation_resume",
        NULL                  AS "rna",
        siret                 AS "siret",
        site_web              AS "site_web",
        _di_source_id         AS "source",
        telephone             AS "telephone",
        typologie             AS "typologie",
        CAST(NULL AS BOOLEAN) AS "antenne",
        CAST(NULL AS TEXT []) AS "labels_autres",
        CAST(NULL AS TEXT []) AS "thematiques",
        CASE
            WHEN typologie = 'AFPA' THEN ARRAY['afpa']
            WHEN typologie = 'ML' THEN ARRAY['mission-locale']
        END                   AS "labels_nationaux"
    FROM structures
)

SELECT * FROM final
