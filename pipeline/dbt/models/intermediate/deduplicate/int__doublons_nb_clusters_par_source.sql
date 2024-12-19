WITH source_cluster_count AS (
    SELECT DISTINCT
        source_1 AS source,
        id
    FROM {{ ref('int__doublons_paires_structures' ) }}
    UNION
    SELECT DISTINCT
        source_2 AS source,
        id
    FROM {{ ref('int__doublons_paires_structures' ) }}
),

final AS (
    SELECT
        CAST('{{ run_started_at.strftime("%Y-%m-%d") }}' AS DATE) AS date_day,
        source,
        COUNT(DISTINCT id)         AS cluster_count
    FROM
        source_cluster_count
    GROUP BY
        source
    ORDER BY
        cluster_count DESC
)

SELECT * FROM final
