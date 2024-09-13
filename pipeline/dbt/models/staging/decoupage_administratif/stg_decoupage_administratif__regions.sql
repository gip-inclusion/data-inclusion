WITH source AS (
    {{ stg_source_header('decoupage_administratif', 'regions') }}
),

final AS (
    SELECT
        code AS "code",
        nom  AS "nom"
    FROM source
    ORDER BY code
)

SELECT * FROM final
