WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        'mes-aides'         AS "source",
        'mes-aides--' || id AS "id",
        longitude           AS "longitude",
        latitude            AS "latitude",
        NULL                AS "complement_adresse",
        adresse             AS "adresse",
        ville_nom           AS "commune",
        code_postal         AS "code_postal",
        code_insee          AS "code_insee"
    FROM garages
    WHERE
        en_ligne
)

SELECT * FROM final
