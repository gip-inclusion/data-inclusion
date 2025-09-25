-- noqa: disable=all

WITH structures AS (
    SELECT * FROM {{ ref('int__structures_v1') }}
    WHERE nom IS NOT NULL
),

final AS (
    SELECT
        cluster.*,
        structures.source,
        COUNT(*) OVER (PARTITION BY cluster.cluster_id) AS size
    FROM
        processings.deduplicate(
            TO_JSONB(
                (
                    SELECT JSONB_AGG(TO_JSONB(structures) || JSONB_BUILD_OBJECT('_di_surrogate_id', structures.id))
                    FROM structures
                )
            )
        ) AS cluster
    INNER JOIN structures ON structures.id = structure_id
)

SELECT * FROM final

-- noqa: enable=all
