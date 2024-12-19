WITH source_clusters AS (
    SELECT
        id,
        ARRAY_AGG(DISTINCT source_1 ORDER BY source_1) AS sources
    FROM {{ ref('int__doublons_paires_structures')}}
    GROUP BY id
),

final AS (
    SELECT
        CAST('{{ run_started_at.strftime("%Y-%m-%d") }}' AS DATE) AS date_day,
        UNNEST(sources)            AS source,
        COUNT(*)                   AS cluster_count
    FROM
        source_clusters
    WHERE
        ARRAY_LENGTH(sources, 1) > 1
    GROUP BY
        source
    ORDER BY
        cluster_count DESC
)

SELECT * FROM final
