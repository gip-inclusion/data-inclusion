WITH cluster_structures AS (
    SELECT DISTINCT
        id,
        structure_id,
        size
    FROM {{ ref('int__doublons_structures') }}
),

structure_sources AS (
    SELECT
        _di_surrogate_id AS id,
        source
    FROM
        {{ ref('int__union_structures') }}
),

structure_pairs AS (
    SELECT
        c1.id,
        s1.source       AS source_1,
        c1.structure_id AS structure_id_1,
        s2.source       AS source_2,
        c2.structure_id AS structure_id_2,
        c1.size
    FROM
        cluster_structures AS c1
    INNER JOIN
        -- The '<' operator ensures we keep a correct order, that all unordered combinations
        -- are considered and that we don't pair a structure with itself
        cluster_structures AS c2 ON c1.id = c2.id AND c1.structure_id < c2.structure_id
    INNER JOIN
        structure_sources AS s1 ON c1.structure_id = s1.id
    INNER JOIN
        structure_sources AS s2 ON c2.structure_id = s2.id
),

final AS (
    SELECT *
    FROM
        structure_pairs
    ORDER BY
        id, structure_id_1, structure_id_2
)

SELECT * FROM final
