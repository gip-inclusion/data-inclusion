WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

labels_nationaux AS (
    SELECT
        structures.id                     AS structure_id,
        ARRAY_AGG(labels_nationaux.value) AS labels_nationaux
    FROM structures,
        UNNEST(structures.labels_nationaux) AS label_dora
    LEFT JOIN {{ ref('labels_nationaux') }} AS labels_nationaux ON label_dora = labels_nationaux.value
    WHERE labels_nationaux.value IS NOT NULL
    GROUP BY 1
),

final AS (
    SELECT
        structures.accessibilite          AS "accessibilite",
        structures.id                     AS "adresse_id",
        structures.antenne                AS "antenne",
        structures.courriel               AS "courriel",
        CAST(structures.date_maj AS DATE) AS "date_maj",
        structures.horaires_ouverture     AS "horaires_ouverture",
        structures.id                     AS "id",
        structures.labels_autres          AS "labels_autres",
        labels_nationaux.labels_nationaux AS "labels_nationaux",
        structures.lien_source            AS "lien_source",
        structures.nom                    AS "nom",
        structures.presentation_detail    AS "presentation_detail",
        structures.presentation_resume    AS "presentation_resume",
        structures.rna                    AS "rna",
        structures.siret                  AS "siret",
        structures.site_web               AS "site_web",
        structures._di_source_id          AS "source",
        structures.telephone              AS "telephone",
        CAST(NULL AS TEXT [])             AS "thematiques",
        structures.typologie              AS "typologie"
    FROM structures
    LEFT JOIN labels_nationaux ON structures.id = labels_nationaux.structure_id
)

SELECT * FROM final
