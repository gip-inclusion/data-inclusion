WITH communes AS (
    {{ stg_source_header('decoupage_administratif', 'communes') }}
),

districts AS (
    {{ stg_source_header('decoupage_administratif', 'districts') }}
),

clean_communes AS (
    SELECT
        communes.code                       AS "code",
        communes.nom                        AS "nom",
        communes."codeRegion"               AS "region",
        communes."codeDepartement"          AS "departement",
        communes."codeEpci"                 AS "siren_epci",
        ST_GEOMFROMGEOJSON(communes.centre) AS "centre",
        communes."codesPostaux"             AS "codes_postaux"
    FROM communes
),

clean_districts AS (
    SELECT
        districts.code                       AS "code",
        districts.nom                        AS "nom",
        districts."codeRegion"               AS "region",
        districts."codeDepartement"          AS "departement",
        NULL                                 AS "siren_epci",
        ST_GEOMFROMGEOJSON(districts.centre) AS "centre",
        districts."codesPostaux"             AS "codes_postaux"
    FROM districts
),

final AS (
    SELECT * FROM clean_communes
    UNION ALL
    SELECT * FROM clean_districts
)

SELECT * FROM final
