WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        'mes-aides'         AS "source",
        'mes-aides--' || id AS "id",
        NULL                AS "longitude",
        NULL                AS "latitude",
        NULL                AS "complement_adresse",
        adresse             AS "adresse",
        ville__nom          AS "commune",
        ville__code_postal  AS "code_postal",
        ville__code_insee   AS "code_insee"
    FROM garages
    WHERE
        en_ligne
)

SELECT * FROM final
