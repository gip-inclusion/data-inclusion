WITH annotation_dataset AS (
    SELECT * FROM {{ source('data_inclusion', 'annotation_dataset') }}
),

annotation_datasetrow AS (
    SELECT * FROM {{ source('data_inclusion', 'annotation_datasetrow') }}
),

annotation_annotation AS (
    SELECT * FROM {{ source('data_inclusion', 'annotation_annotation') }}
),

final AS (
    SELECT DISTINCT ON (3)
        annotation_annotation.siret                                                     AS "siret",
        annotation_annotation.is_parent                                                 AS "antenne",
        annotation_dataset.source || '-' || (annotation_datasetrow.data ->> 'id')::TEXT AS "surrogate_id"
    FROM
        annotation_annotation
    INNER JOIN
        annotation_datasetrow ON
            annotation_annotation.row_id = annotation_datasetrow.id
    INNER JOIN
        annotation_dataset ON
            annotation_datasetrow.dataset_id = annotation_dataset.id
    WHERE
        NOT annotation_annotation.closed
        AND NOT annotation_annotation.irrelevant
        AND NOT annotation_annotation.skipped
        AND annotation_dataset.source != ''
    ORDER BY
        3 ASC,
        annotation_annotation.created_at DESC
)

SELECT * FROM final
