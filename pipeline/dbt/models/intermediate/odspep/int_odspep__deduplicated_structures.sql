WITH structures AS (
    SELECT * FROM {{ ref('int_odspep__structures') }}
),

final AS (
    {{
        dbt_utils.deduplicate(
            relation='structures',
            partition_by='unaccent(lower(commune)), unaccent(lower(code_insee)), unaccent(lower(nom)), unaccent(lower(adresse))',
            order_by='date_maj DESC'
        )
    }}
)

SELECT * FROM final
