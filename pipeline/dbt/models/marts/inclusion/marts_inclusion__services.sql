WITH services AS (
    SELECT * FROM {{ ref('int__union_services__enhanced') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__union_services__enhanced'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                ]
            )
        }}
    FROM services
)

SELECT * FROM final
