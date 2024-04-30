WITH source AS (
    SELECT * FROM {{ source('insee', 'communes') }}
),

communes AS (
    SELECT
        "COM"     AS "code",
        "LIBELLE" AS "libelle",
        CASE "TYPECOM"
            WHEN 'COM' THEN 'commune'
            WHEN 'COMA' THEN 'commune-associee'
            WHEN 'COMD' THEN 'commune-deleguee'
            WHEN 'ARM' THEN 'arrondissement-municipal'
        END       AS "type_commune"
    FROM source
),

-- drop communes deleguees
-- they use the same code as their parent commune
-- this creates duplicates
final AS (
    SELECT *
    FROM communes
    WHERE type_commune != 'commune-deleguee'
)

SELECT * FROM final
