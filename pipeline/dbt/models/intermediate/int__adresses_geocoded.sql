WITH adresses AS (
    SELECT * FROM {{ ref('int__adresses') }}
),

geocoded_results AS (
    SELECT * FROM {{ ref('int_extra__geocoded_results') }}
),

final AS (
    SELECT
        adresses.*,
        geocoded_results.result_score,
        geocoded_results.result_citycode
    FROM adresses
    LEFT JOIN geocoded_results ON adresses._di_surrogate_id = geocoded_results._di_surrogate_id
)

SELECT * FROM final
