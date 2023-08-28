WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

annotation_annotations AS (
    SELECT * FROM {{ source('data_inclusion', 'annotation_annotation') }}
),

latest_annotation_by_surrogate_id AS (
    SELECT DISTINCT ON (1)
        annotation_annotations.di_surrogate_id AS "_di_surrogate_id",
        annotation_annotations.siret           AS "siret",
        annotation_annotations.is_parent       AS "is_parent",
        annotation_annotations.skipped         AS "skipped",
        annotation_annotations.closed          AS "closed",
        annotation_annotations.irrelevant      AS "irrelevant"
    FROM annotation_annotations
    ORDER BY
        annotation_annotations.di_surrogate_id ASC,
        annotation_annotations.created_at DESC
),

enhanced_structures AS (
    SELECT
        structures.source                            AS "source",
        latest_annotation_by_surrogate_id.siret      AS "siret",
        latest_annotation_by_surrogate_id.is_parent  AS "is_parent",
        latest_annotation_by_surrogate_id.skipped    AS "skipped",
        latest_annotation_by_surrogate_id.closed     AS "closed",
        latest_annotation_by_surrogate_id.irrelevant AS "irrelevant"
    FROM structures
    LEFT JOIN latest_annotation_by_surrogate_id
        ON structures._di_surrogate_id = latest_annotation_by_surrogate_id._di_surrogate_id
),

final AS (
    SELECT
        source                                                       AS "source",
        COUNT(*) FILTER (WHERE siret IS NOT NULL)                    AS "row_with_siret_count",
        100.0 * COUNT(*) FILTER (WHERE siret IS NOT NULL) / COUNT(*) AS "row_with_siret_percentage",
        COUNT(*) FILTER (WHERE skipped)                              AS "row_skipped_count",
        100.0 * COUNT(*) FILTER (WHERE skipped) / COUNT(*)           AS "row_skipped_percentage",
        COUNT(*) FILTER (WHERE closed)                               AS "row_closed_count",
        100.0 * COUNT(*) FILTER (WHERE closed) / COUNT(*)            AS "row_closed_percentage",
        COUNT(*) FILTER (WHERE irrelevant)                           AS "row_irrelevant_count",
        100.0 * COUNT(*) FILTER (WHERE irrelevant) / COUNT(*)        AS "row_irrelevant_percentage",
        COUNT(*) FILTER (WHERE is_parent)                            AS "row_antenne_count",
        100.0 * COUNT(*) FILTER (WHERE is_parent) / COUNT(*)         AS "row_antenne_percentage"
    FROM enhanced_structures
    GROUP BY source
)

SELECT * FROM final
