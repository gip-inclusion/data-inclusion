WITH source AS (
    SELECT * FROM {{ source('sirene', 'etablissement_succession') }}
)

SELECT
    "siretEtablissementPredecesseur"            AS "siret_etablissement_predecesseur",
    "siretEtablissementSuccesseur"              AS "siret_etablissement_successeur",
    TO_DATE("dateLienSuccession", 'YYYY-MM-DD') AS "date_lien_succession"
FROM source
