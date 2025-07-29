WITH source AS (
    SELECT * FROM {{ source('sirene', 'stock_etablissement') }}
)

SELECT
    siret AS "siret",
    CASE "etatAdministratifEtablissement"
        WHEN 'A' THEN 'actif'
        WHEN 'F' THEN 'fermé'
    END   AS "etat_administratif_etablissement"
FROM source
