WITH agences AS (
    SELECT * FROM {{ ref('stg_france_travail__agences') }}
),

communes AS (
    SELECT * FROM {{ source('insee', 'communes') }}
),

-- exclude communes déléguées (duplicated codes) ; should maybe be a permanent INSEE view ?
filtered_communes AS (
    SELECT *
    FROM communes
    WHERE "TYPECOM" != 'COMD'
),


final AS (
    SELECT
        agences.id                  AS "id",
        agences.longitude           AS "longitude",
        agences.latitude            AS "latitude",
        agences._di_source_id       AS "source",
        agences.complement_adresse  AS "complement_adresse",
        agences.adresse             AS "adresse",
        agences.code_postal         AS "code_postal",
        agences.code_insee          AS "code_insee",
        filtered_communes."LIBELLE" AS "commune"
    FROM agences
    LEFT JOIN filtered_communes ON agences.code_insee = filtered_communes."COM"
)

SELECT * FROM final
