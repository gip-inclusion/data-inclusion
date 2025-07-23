WITH source AS (
    SELECT * FROM {{ source('insee', 'sirene_stock_etablissement') }}
)

SELECT siret
FROM source
