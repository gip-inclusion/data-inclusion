WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

doublons AS (
    SELECT * FROM {{ ref('int__doublons_structures') }}
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
        }},
        doublons.cluster_id
    FROM structures
    LEFT JOIN doublons ON structures._di_surrogate_id = doublons.structure_id
)

SELECT * FROM final
