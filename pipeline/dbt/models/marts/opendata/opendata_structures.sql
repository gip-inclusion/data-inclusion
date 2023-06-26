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
                except=['courriel', 'telephone'])
        }},
        CASE
            WHEN structures._di_email_is_pii THEN {{ obfuscate('structures.courriel') }}
            ELSE structures.courriel
        END AS "courriel",
        CASE
            WHEN structures._di_email_is_pii THEN {{ obfuscate('structures.telephone') }}
            ELSE structures.telephone
        END AS "telephone"
    FROM structures
    WHERE structures.source NOT IN ('soliguide', 'siao', 'finess')
)

SELECT * FROM final
