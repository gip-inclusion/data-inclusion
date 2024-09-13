WITH source AS (
    {{ stg_source_header('decoupage_administratif', 'departements') }}
),

final AS (
    SELECT
        code         AS "code",
        nom          AS "nom",
        "codeRegion" AS "code_region"
    FROM source
    ORDER BY code
)

SELECT * FROM final
