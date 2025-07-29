WITH source AS (
    SELECT * FROM {{ source('etat_civil', 'prenoms') }}
),

cleaned AS (
    SELECT
        CASE CAST(sexe AS INT)
            WHEN 1 THEN 'masculin'
            WHEN 2 THEN 'feminin'
        END                 AS "sexe",
        LOWER(preusuel)     AS "prenom",
        CAST(annais AS INT) AS "annee_naissance",
        nombre
    FROM source
    WHERE preusuel != '_PRENOMS_RARES'
),

filtered AS (
    SELECT prenom
    FROM cleaned
    WHERE
        LENGTH(prenom) > 2
        AND prenom !~* 'saint'
    GROUP BY 1
    HAVING SUM(nombre) > 100
    ORDER BY 1 ASC
)

SELECT * FROM filtered
