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
    SELECT DISTINCT ON (1, 4)
        annotation_dataset.source,
        annotation_annotation.siret,
        annotation_annotation.is_parent,
        annotation_datasetrow.data ->> 'id' AS "id"
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
        annotation_dataset.source ASC,
        annotation_datasetrow.data ->> 'id' ASC,
        annotation_annotation.created_at DESC
)

SELECT * FROM final
