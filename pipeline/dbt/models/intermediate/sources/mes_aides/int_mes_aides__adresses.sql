WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        id              AS "id",
        ville_nom       AS "commune",
        code_postal     AS "code_postal",
        code_insee      AS "code_insee",
        adresse         AS "adresse",
        NULL            AS "complement_adresse",
        ville_longitude AS "longitude",
        ville_latitude  AS "latitude",
        _di_source_id   AS "source"
    FROM garages
)

SELECT * FROM final
