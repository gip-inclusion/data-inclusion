WITH siaes AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__siaes') }}
),

organisations AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

structures AS (
    SELECT * FROM siaes
    UNION
    SELECT * FROM organisations
),

final AS (
    SELECT
        id                             AS "id",
        longitude                      AS "longitude",
        latitude                       AS "latitude",
        _di_source_id                  AS "source",
        NULLIF(complement_adresse, '') AS "complement_adresse",
        NULLIF(adresse, '')            AS "adresse",
        NULLIF(commune, '')            AS "commune",
        NULLIF(code_postal, '')        AS "code_postal",
        NULLIF(code_insee, '')         AS "code_insee"
    FROM structures
)

SELECT * FROM final
