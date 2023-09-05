WITH services AS (
    SELECT * FROM {{ ref('int__union_services__enhanced') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__union_services__enhanced'),
                except=['courriel', 'telephone'])
        }},
        {{ obfuscate('courriel') }} AS "courriel",
        {{ obfuscate('telephone') }} AS "telephone"
    FROM services
    WHERE services.source NOT IN ('soliguide', 'agefiph')
)

SELECT * FROM final
