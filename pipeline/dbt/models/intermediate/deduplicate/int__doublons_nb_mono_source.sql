WITH groups AS (
    SELECT
        cluster_id,
        source,
        COUNT(*) AS total_from_source_in_cluster
    FROM {{ ref('int__doublons_structures') }}
    GROUP BY cluster_id, source
    HAVING COUNT(*) >= 2
),


counts AS (
    SELECT
        source,
        SUM(total_from_source_in_cluster) AS count
    FROM groups
    GROUP BY source
    ORDER BY count DESC
),

total_rows AS (
    SELECT
        source,
        COUNT(*) AS total
    FROM {{ ref('int__union_structures') }}
    GROUP BY source
),

final AS (
    SELECT
        counts.source,
        counts.count,
        ROUND(counts.count * 100 / total_rows.total, 2) AS percent
    FROM counts
    INNER JOIN total_rows
        ON counts.source = total_rows.source
    ORDER BY counts.source
)

SELECT * FROM final
