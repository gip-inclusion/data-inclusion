WITH source AS (
    {{ stg_source_header('decoupage_administratif', 'communes') }}
),

final AS (
    SELECT
        code                       AS "code",
        nom                        AS "nom",
        "codeRegion"               AS "code_region",
        "codeDepartement"          AS "code_departement",
        "codeEpci"                 AS "code_epci",
        ST_GEOMFROMGEOJSON(centre) AS "centre",
        "codesPostaux"             AS "codes_postaux"
    FROM source
    ORDER BY code
)

SELECT * FROM final
