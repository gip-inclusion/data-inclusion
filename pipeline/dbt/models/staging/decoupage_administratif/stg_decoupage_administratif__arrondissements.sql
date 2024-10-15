WITH source AS (
    {{ stg_source_header('decoupage_administratif', 'arrondissements') }}
),

final AS (
    SELECT
        code                       AS "code",
        nom                        AS "nom",
        "codeRegion"               AS "code_region",
        "codeDepartement"          AS "code_departement",
        CASE
            WHEN LEFT(code, 3) = '751' THEN '75056'  -- Paris
            WHEN LEFT(code, 3) = '693' THEN '69123'  -- Lyon
            WHEN LEFT(code, 3) = '132' THEN '13055'  -- Marseille
        END                        AS "code_commune",
        ST_GEOMFROMGEOJSON(centre) AS "centre",
        "codesPostaux"             AS "codes_postaux"
    FROM source
    ORDER BY code
)

SELECT * FROM final
