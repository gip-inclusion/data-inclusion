WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

final AS (
    SELECT
        id                          AS "id",
        liaisons_villes_nom         AS "commune",
        liaisons_villes_code_postal AS "code_postal",
        NULL                        AS "code_insee",
        NULL                        AS "adresse",
        NULL                        AS "complement_adresse",
        NULL::FLOAT                 AS "longitude",
        NULL::FLOAT                 AS "latitude",
        'mes-aides'                 AS "source"
    FROM permis_velo
)

SELECT * FROM final
