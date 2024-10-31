WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__union_structures__enhanced'),
                except=[
                    '_di_adresse_surrogate_id',
                    '_di_email_is_pii',
                    'adresse_id',
                ]
            )
        }}
    FROM structures
    WHERE structures.source != 'finess'
)

SELECT * FROM final
