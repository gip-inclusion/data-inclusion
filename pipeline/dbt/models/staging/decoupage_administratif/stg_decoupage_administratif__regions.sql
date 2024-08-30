WITH regions AS (
    {{ stg_source_header('decoupage_administratif', 'regions') }}
),

final AS (
    SELECT * FROM regions ORDER BY nom
)

SELECT * FROM final
