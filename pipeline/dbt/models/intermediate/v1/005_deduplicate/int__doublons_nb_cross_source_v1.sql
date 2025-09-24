WITH src AS (
    SELECT * FROM {{ ref('int__doublons_structures_v1') }}
),

structures AS (
    SELECT * FROM {{ ref('int__union_structures_v1') }}
),

cross_counts AS (
    SELECT
        a.source                       AS source_1,
        b.source                       AS source_2,
        COUNT(DISTINCT a.structure_id) AS nb_1,
        COUNT(DISTINCT b.structure_id) AS nb_2
    FROM src AS a
    INNER JOIN src AS b
        ON
            a.cluster_id = b.cluster_id
            AND a.source < b.source
    GROUP BY a.source, b.source
),

total_counts AS (
    SELECT
        source,
        COUNT(*) AS total
    FROM structures
    GROUP BY source
),

final AS (
    SELECT
        cc.*,
        ROUND(cc.nb_1 * 100.0 / tc1.total, 2) AS percent_1,
        ROUND(cc.nb_2 * 100.0 / tc2.total, 2) AS percent_2
    FROM cross_counts AS cc
    INNER JOIN total_counts AS tc1 ON cc.source_1 = tc1.source
    INNER JOIN total_counts AS tc2 ON cc.source_2 = tc2.source
    ORDER BY cc.source_1, cc.source_2
)

SELECT * FROM final
