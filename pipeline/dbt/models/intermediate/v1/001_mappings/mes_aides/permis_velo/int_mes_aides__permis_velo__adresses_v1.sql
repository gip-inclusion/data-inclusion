WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

final AS (
    SELECT
        'mes-aides'                 AS "source",
        'mes-aides--' || id         AS "id",
        liaisons_villes_nom         AS "commune",
        liaisons_villes_code_postal AS "code_postal",
        -- Les aides permis/velo n'ont pas d'adresse
        NULL                        AS "code_insee",
        NULL                        AS "adresse",
        NULL                        AS "complement_adresse",
        CAST(NULL AS FLOAT)         AS "longitude",
        CAST(NULL AS FLOAT)         AS "latitude"
    FROM permis_velo
    WHERE
        en_ligne
)

SELECT * FROM final
