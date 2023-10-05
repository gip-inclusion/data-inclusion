WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

final AS (
    SELECT
        lieux.lieu_id                     AS "id",
        lieux._di_source_id               AS "source",
        lieux.position_coordinates_x      AS "longitude",
        lieux.position_coordinates_y      AS "latitude",
        lieux.position_complement_adresse AS "complement_adresse",
        lieux.position_ville              AS "commune",
        lieux.position_adresse            AS "adresse",
        lieux.position_code_postal        AS "code_postal",
        NULL                              AS "code_insee"
    FROM lieux
    ORDER BY 1
)

SELECT * FROM final
