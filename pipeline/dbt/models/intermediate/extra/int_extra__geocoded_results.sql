{{
    config(
        materialized="view"
    )
}}

WITH geocoded_results AS (
    SELECT * FROM {{ source('data_inclusion', 'extra__geocoded_results') }}
),

final AS (
    SELECT *
    FROM geocoded_results
    WHERE result_id IS NOT NULL
)

SELECT * FROM final
