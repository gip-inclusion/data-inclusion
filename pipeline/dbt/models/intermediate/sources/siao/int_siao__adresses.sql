WITH etablissements AS (
    SELECT * FROM {{ ref('stg_siao__etablissements') }}
),

final AS (
    SELECT
        id            AS "id",
        ville         AS "commune",
        code_postal   AS "code_postal",
        NULL          AS "code_insee",
        adresse       AS "adresse",
        NULL          AS "complement_adresse",
        NULL::FLOAT   AS "longitude",
        NULL::FLOAT   AS "latitude",
        _di_source_id AS "source"
    FROM etablissements
)

SELECT * FROM final
