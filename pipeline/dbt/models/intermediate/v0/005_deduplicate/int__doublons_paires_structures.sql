WITH cluster_structures AS (
    SELECT * FROM {{ ref('int__doublons_structures') }}
),

structure_pairs AS (
    SELECT
        c1.cluster_id,
        c1.source       AS source_1,
        c1.structure_id AS structure_id_1,
        c2.source       AS source_2,
        c2.structure_id AS structure_id_2,
        c1.size
    FROM
        cluster_structures AS c1
    INNER JOIN
        -- The '<' operator ensures we keep a correct order, that all unordered combinations
        -- are considered and that we don't pair a structure with itself
        cluster_structures AS c2 ON c1.cluster_id = c2.cluster_id AND c1.structure_id < c2.structure_id
),

final AS (
    SELECT *
    FROM
        structure_pairs
    ORDER BY
        cluster_id, structure_id_1, structure_id_2
)

SELECT * FROM final
