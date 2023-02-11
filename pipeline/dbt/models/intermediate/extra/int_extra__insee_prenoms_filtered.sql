WITH firstname_counts_by_year_and_date AS (
    SELECT *
    FROM {{ source('data_inclusion', 'external_insee_fichier_prenoms') }}
),

final AS (
    SELECT DISTINCT LOWER(preusuel) AS prenom
    FROM firstname_counts_by_year_and_date
    WHERE
        LENGTH(preusuel) > 2
        AND nombre > 100
        AND preusuel != 'SAINT'
)

SELECT * FROM final
