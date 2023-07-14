WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        id                                             AS "id",
        ville                                          AS "commune",
        NULL                                           AS "code_insee",
        longitude                                      AS "longitude",
        latitude                                       AS "latitude",
        _di_source_id                                  AS "source",
        SUBSTRING(adresse FROM '\d{5}')                AS "code_postal",
        SUBSTRING(adresse FROM '^(.*?) (- .* )?\d{5}') AS "adresse",
        SUBSTRING(adresse FROM '- (.*) \d{5}')         AS "complement_adresse"
    FROM creches
    WHERE avip  -- temporarily limit results to avip
)

SELECT * FROM final
