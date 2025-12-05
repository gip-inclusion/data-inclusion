WITH agences AS (
    SELECT * FROM {{ ref('stg_france_travail__agences') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

final AS (
    SELECT
        'france-travail'                                 AS "source",
        'france-travail--' || agences.code               AS "id",
        agences.adresse_principale__gps_lon              AS "longitude",
        agences.adresse_principale__gps_lat              AS "latitude",
        agences.adresse_principale__ligne_3              AS "complement_adresse",
        communes.nom                                     AS "commune",
        agences.adresse_principale__ligne_4              AS "adresse",
        agences.adresse_principale__bureau_distributeur  AS "code_postal",
        agences.adresse_principale__commune_implantation AS "code_insee"
    FROM agences
    LEFT JOIN communes ON agences.adresse_principale__commune_implantation = communes.code
)

SELECT * FROM final
