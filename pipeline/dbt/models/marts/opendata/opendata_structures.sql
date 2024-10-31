WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__union_structures__enhanced'),
                except=['courriel', 'telephone'])
        }},
        -- obfuscate email (& telephone) if it is potentially personal
        NULLIF(structures.courriel, courriels_personnels.courriel) AS "courriel",
        CASE
            WHEN structures.courriel != courriels_personnels.courriel
                THEN structures.telephone
        END                                                        AS "telephone"
    FROM structures
    LEFT JOIN courriels_personnels
        ON structures.courriel = courriels_personnels.courriel
    -- exclude specific sources from open data
    WHERE structures.source NOT IN ('soliguide', 'finess', 'agefiph')
)

SELECT * FROM final
