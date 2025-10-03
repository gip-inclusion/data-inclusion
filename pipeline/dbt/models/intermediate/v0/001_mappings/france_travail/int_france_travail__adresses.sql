WITH agences AS (
    SELECT * FROM {{ ref('stg_france_travail__agences') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

final AS (
    SELECT
        agences.id                 AS "id",
        agences.longitude          AS "longitude",
        agences.latitude           AS "latitude",
        'france-travail'           AS "source",
        agences.complement_adresse AS "complement_adresse",
        agences.adresse            AS "adresse",
        agences.code_postal        AS "code_postal",
        agences.code_insee         AS "code_insee",
        communes.nom               AS "commune"
    FROM agences
    LEFT JOIN communes ON agences.code_insee = communes.code
)

SELECT * FROM final
