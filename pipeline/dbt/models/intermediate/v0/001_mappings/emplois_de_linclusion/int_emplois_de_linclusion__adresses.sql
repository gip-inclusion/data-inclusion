WITH structures AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

final AS (
    SELECT
        id                      AS "id",
        longitude               AS "longitude",
        latitude                AS "latitude",
        'emplois-de-linclusion' AS "source",
        complement_adresse      AS "complement_adresse",
        adresse                 AS "adresse",
        commune                 AS "commune",
        code_postal             AS "code_postal",
        NULL                    AS "code_insee"
    FROM structures
)

SELECT * FROM final
