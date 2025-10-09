WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

final AS (
    SELECT
        'soliguide'                                                                            AS "source",
        'soliguide--' || lieux.lieu_id                                                         AS "id",
        lieux.position__coordinates__x                                                         AS "longitude",
        lieux.position__coordinates__y                                                         AS "latitude",
        lieux.position__additional_information                                                 AS "complement_adresse",
        NULLIF(BTRIM(REGEXP_REPLACE(lieux.position__address, ', \d\d\d\d\d.*$', ''), ','), '') AS "adresse",
        lieux.position__city                                                                   AS "commune",
        lieux.position__postal_code                                                            AS "code_postal",
        -- TODO/UP(2025-10-01) : position__city_code still only contains zip codes
        NULL                                                                                   AS "code_insee"
    FROM lieux
)

SELECT * FROM final
