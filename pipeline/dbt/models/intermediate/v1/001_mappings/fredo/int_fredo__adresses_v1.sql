WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

final AS (
    SELECT
        'fredo'         AS "source",
        'fredo--' || id AS "id",
        commune         AS "commune",
        NULL            AS "code_insee",
        latitude        AS "latitude",
        longitude       AS "longitude",
        code_postal     AS "code_postal",
        adresse         AS "adresse",
        NULL            AS "complement_adresse"
    FROM structures
)

SELECT * FROM final
