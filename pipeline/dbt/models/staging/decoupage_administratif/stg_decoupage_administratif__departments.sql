WITH departements AS (
    {{ stg_source_header('decoupage_administratif', 'departements') }}
),

final AS (
    SELECT * FROM departements ORDER BY code
)

SELECT * FROM final
