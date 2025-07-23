WITH source AS (
    SELECT * FROM {{ source('insee', 'sirene_etablissement_succession') }}
)

SELECT
    "siretEtablissementPredecesseur" AS "siret_etablissement_predecesseur",
    "siretEtablissementSuccesseur"   AS "siret_etablissement_successeur",
    "dateLienSuccession"             AS "date_lien_succession",
    "continuiteEconomique"           AS "continuite_economique"
FROM source
