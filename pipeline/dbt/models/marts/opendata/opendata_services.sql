WITH services AS (
    SELECT * FROM {{ ref('int__union_services__enhanced') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__union_services__enhanced'),
                except=['courriel', 'telephone'])
        }},
        -- obfuscate email (& telephone) if it is potentially personal
        NULLIF(services.courriel, courriels_personnels.courriel) AS "courriel",
        CASE
            WHEN services.courriel != courriels_personnels.courriel
                THEN services.telephone
        END                                                      AS "telephone"
    FROM services
    LEFT JOIN courriels_personnels
        ON services.courriel = courriels_personnels.courriel
    -- exclude specific sources from open data
    WHERE services.source NOT IN ('soliguide', 'agefiph', 'data-inclusion')
)

SELECT * FROM final
