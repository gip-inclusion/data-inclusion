WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

final AS (
    SELECT
        lieu_id                                                                          AS "id",
        _di_source_id                                                                    AS "source",
        position__coordinates__x                                                         AS "longitude",
        position__coordinates__y                                                         AS "latitude",
        position__additional_information                                                 AS "complement_adresse",
        position__city                                                                   AS "commune",
        NULLIF(BTRIM(REGEXP_REPLACE(position__address, ', \d\d\d\d\d.*$', ''), ','), '') AS "adresse",
        position__postal_code                                                            AS "code_postal",
        -- TODO: use position__city_code
        -- currently the field contains a majority of postal codes...
        -- update(2024-08-07) : this is still the case.
        NULL                                                                             AS "code_insee"
    FROM lieux
    ORDER BY 1
)

SELECT * FROM final
