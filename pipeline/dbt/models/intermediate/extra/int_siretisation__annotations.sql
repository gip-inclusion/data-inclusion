{%
    set tables_exist = (adapter.get_relation(
        database=source('data_inclusion', 'annotation_dataset').database,
        schema=source('data_inclusion', 'annotation_dataset').schema,
        identifier=source('data_inclusion', 'annotation_dataset').name,
    ) is not none) and (adapter.get_relation(
        database=source('data_inclusion', 'annotation_annotation').database,
        schema=source('data_inclusion', 'annotation_annotation').schema,
        identifier=source('data_inclusion', 'annotation_annotation').name,
    ) is not none)
%}

{% if tables_exist %}

    WITH annotation_dataset AS (
        SELECT * FROM {{ source('data_inclusion', 'annotation_dataset') }}
    ),

    annotation_annotation AS (
        SELECT * FROM {{ source('data_inclusion', 'annotation_annotation') }}
    ),

    final AS (
        SELECT DISTINCT ON (1)
            annotation_annotation.di_surrogate_id AS "_di_surrogate_id",
            annotation_annotation.siret           AS "siret",
            annotation_annotation.is_parent       AS "antenne"
        FROM
            annotation_annotation
        INNER JOIN
            annotation_dataset ON
            annotation_annotation.dataset_id = annotation_dataset.id
        WHERE
            NOT annotation_annotation.closed
            AND NOT annotation_annotation.irrelevant
            AND NOT annotation_annotation.skipped
            AND annotation_dataset.source != ''
        ORDER BY
            annotation_annotation.di_surrogate_id ASC,
            annotation_annotation.created_at DESC
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL                  AS "_di_surrogate_id",
    NULL                  AS "siret",
    CAST(NULL AS BOOLEAN) AS "antenne"
WHERE FALSE

{% endif %}
