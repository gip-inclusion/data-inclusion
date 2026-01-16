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
    WHERE
        -- many soliguide services did have no street-level address info (e.g., town halls)
        (lieux.position__address IS NOT NULL OR lieux.position__additional_information IS NOT NULL)
        AND (lieux.position__city IS NOT NULL OR lieux.position__postal_code IS NOT NULL)
)

SELECT * FROM final
