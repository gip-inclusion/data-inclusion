WITH structures AS (
    SELECT * FROM {{ ref('int__validated_structures') }}
),

final AS (
    -- obfuscate email if it contains PII
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__validated_structures'),
                except=['courriel'])
        }},
        CASE
            WHEN structures._di_email_is_pii THEN '***'
            ELSE structures.courriel
        END AS "courriel"
    FROM structures
    WHERE structures.source NOT IN ('soliguide', 'siao', 'finess', 'immersion-facilitee')
)

SELECT * FROM final
