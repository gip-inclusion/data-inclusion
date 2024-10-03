WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

final AS (
    SELECT
        id                   AS "id",
        liaisons_ville_nom   AS "commune",
        liaisons_code_postal AS "code_postal",
        NULL                 AS "code_insee",
        NULL                 AS "adresse",
        NULL                 AS "complement_adresse",
        CAST(NULL AS FLOAT)  AS "longitude",
        CAST(NULL AS FLOAT)  AS "latitude",
        _di_source_id        AS "source"
    FROM permis_velo
)

SELECT * FROM final
