WITH source_communes AS (
    {{ stg_source_header('decoupage_administratif', 'communes') }}
),

source_arrondissements AS (
    {{ stg_source_header('decoupage_administratif', 'arrondissements') }}
),

communes AS (
    SELECT
        source_communes.code                       AS "code",
        source_communes.nom                        AS "nom",
        source_communes."codeRegion"               AS "code_region",
        source_communes."codeDepartement"          AS "code_departement",
        source_communes."codeEpci"                 AS "code_epci",
        ST_GEOMFROMGEOJSON(source_communes.centre) AS "centre",
        source_communes."codesPostaux"             AS "codes_postaux"
    FROM source_communes
),

arrondissements AS (
    SELECT
        source_arrondissements.code                       AS "code",
        source_arrondissements.nom                        AS "nom",
        source_arrondissements."codeRegion"               AS "code_region",
        source_arrondissements."codeDepartement"          AS "code_departement",
        NULL                                              AS "code_epci",
        ST_GEOMFROMGEOJSON(source_arrondissements.centre) AS "centre",
        source_arrondissements."codesPostaux"             AS "codes_postaux"
    FROM source_arrondissements
),

final AS (
    SELECT * FROM communes
    UNION ALL
    SELECT * FROM arrondissements
    ORDER BY code
)

SELECT * FROM final
