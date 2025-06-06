WITH structures AS (
    SELECT * FROM {{ ref('int__structures') }}
    WHERE nom IS NOT NULL
),

final AS (
    SELECT
        cluster.*,
        structures.source,
        COUNT(*) OVER (PARTITION BY cluster.cluster_id) AS size  -- noqa: references.keywords
    FROM
        processings.deduplicate(TO_JSONB(
            (SELECT JSONB_AGG(ROW_TO_JSON(structures)) FROM structures)  -- noqa: references.qualification
        )) AS cluster
    INNER JOIN structures ON structures._di_surrogate_id = structure_id
)

SELECT * FROM final
