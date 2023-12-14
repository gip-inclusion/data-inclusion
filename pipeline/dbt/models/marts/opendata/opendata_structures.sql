WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

final AS (
    -- obfuscate email if it contains PII
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__union_structures__enhanced'),
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
    WHERE structures.source NOT IN ('soliguide', 'siao', 'finess', 'agefiph', 'data-inclusion')
)

SELECT * FROM final
