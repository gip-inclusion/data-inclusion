WITH source AS (
    SELECT * FROM {{ source('etat_civil', 'prenoms') }}
),

cleaned AS (
    SELECT
        LOWER(prenom) AS "prenom",
        valeur
    FROM source
    WHERE
        prenom != '_PRENOMS_RARES'
        AND niveau_geographique = 'FRANCE'
)

SELECT prenom
FROM cleaned
WHERE
    LENGTH(prenom) > 2
    AND prenom !~* 'saint'
GROUP BY 1
HAVING SUM(valeur) > 100
ORDER BY 1 ASC
