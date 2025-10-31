WITH source AS (
    {{ stg_source_header('decoupage_administratif', 'communes_associees_deleguees') }}),

regions AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__regions') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

communes AS (
    SELECT
        code                       AS "code",
        nom                        AS "nom",
        "codeRegion"               AS "code_region",
        "codeDepartement"          AS "code_departement",
        "codeEpci"                 AS "code_epci",
        ST_GEOMFROMGEOJSON(centre) AS "centre"
    FROM source
    ORDER BY code
),

final AS (
    SELECT
        communes.*,
        regions.nom      AS "nom_region",
        departements.nom AS "nom_departement"
    FROM communes
    INNER JOIN regions ON communes.code_region = regions.code
    INNER JOIN departements ON communes.code_departement = departements.code
)

SELECT * FROM final
