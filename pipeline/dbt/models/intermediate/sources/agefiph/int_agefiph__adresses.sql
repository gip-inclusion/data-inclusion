WITH structures AS (
    SELECT * FROM {{ ref('int_agefiph__structures') }}
)

SELECT
    'agefiph'                    AS "source",
    structures.id                AS "id",
    'Bagneux'                    AS "commune",
    '92220'                      AS "code_postal",
    '92007'                      AS "code_insee",
    '192 avenue Aristide Briand' AS "adresse",
    NULL                         AS "complement_adresse",
    2.32097                      AS "longitude",
    48.794432                    AS "latitude"
FROM structures
