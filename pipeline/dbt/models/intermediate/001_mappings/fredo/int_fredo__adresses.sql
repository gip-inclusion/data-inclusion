WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

final AS (
    SELECT
        id                            AS "id",
        commune                       AS "commune",
        NULL                          AS "code_insee",
        CASE
            WHEN adresse IS NOT NULL OR adresse = 'La Réunion' THEN latitude
        END                           AS "latitude",
        CASE
            WHEN adresse IS NOT NULL OR adresse = 'La Réunion' THEN longitude
        END                           AS "longitude",
        'fredo'                       AS "source",
        code_postal                   AS "code_postal",
        NULLIF(adresse, 'La Réunion') AS "adresse",
        NULL                          AS "complement_adresse"
    FROM structures
)

SELECT * FROM final
