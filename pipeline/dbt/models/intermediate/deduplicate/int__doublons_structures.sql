WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

final AS (
    SELECT
        cluster.*,
        structures.source,
        COUNT(*) OVER (PARTITION BY cluster_id) AS size
    FROM
        processings.deduplicate(TO_JSONB(
            (SELECT JSONB_AGG(ROW_TO_JSON(structures)) FROM structures)  -- noqa: references.qualification
        )) AS cluster
    INNER JOIN structures ON structures._di_surrogate_id = structure_id
)

SELECT * FROM final
