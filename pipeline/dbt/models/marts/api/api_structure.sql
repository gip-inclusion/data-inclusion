WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__enhanced_structures'),
                except=[
                    '_di_adresse_surrogate_id',
                    '_di_annotated_antenne',
                    '_di_annotated_siret',
                    '_di_email_is_pii',
                    'adresse_id',
                ]
            )
        }}
    FROM structures
    WHERE structures.source NOT IN ('siao', 'finess')
)

SELECT * FROM final
