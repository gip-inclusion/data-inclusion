WITH services AS (
    SELECT * FROM {{ ref('int__validated_services') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__validated_services'),
                except=['courriel', 'telephone'])
        }},
        {{ obfuscate('courriel') }} AS "courriel",
        {{ obfuscate('telephone') }} AS "telephone"
    FROM services
    WHERE services.source != 'soliguide'
)

SELECT * FROM final
