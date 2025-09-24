WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

final AS (
    SELECT
        'soliguide'                            AS "source",
        'soliguide--' || lieux.lieu_id         AS "id",
        lieux.position__coordinates__x         AS "longitude",
        lieux.position__coordinates__y         AS "latitude",
        lieux.position__additional_information AS "complement_adresse",
        lieux.position__address                AS "adresse",
        lieux.position__city                   AS "commune",
        lieux.position__postal_code            AS "code_postal",
        NULL                                   AS "code_insee"
    FROM lieux
)

SELECT * FROM final
