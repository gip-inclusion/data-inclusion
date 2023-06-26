WITH organisations AS (
    SELECT * FROM {{ ref('stg_cd35__organisations') }}
),

final AS (
    SELECT
        id            AS "id",
        org_ville     AS "commune",
        org_cp        AS "code_postal",
        NULL          AS "code_insee",
        org_adres     AS "adresse",
        NULL          AS "complement_adresse",
        org_longitude AS "longitude",
        org_latitude  AS "latitude",
        _di_source_id AS "source"
    FROM organisations
)

SELECT * FROM final
