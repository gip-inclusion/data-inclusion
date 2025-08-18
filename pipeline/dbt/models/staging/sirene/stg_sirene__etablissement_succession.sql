WITH source AS (
    SELECT * FROM {{ source('sirene', 'etablissement_succession') }}
)

SELECT
    LPAD("siretEtablissementPredecesseur", 14, '0') AS "siret_etablissement_predecesseur",
    LPAD("siretEtablissementSuccesseur", 14, '0')   AS "siret_etablissement_successeur",
    TO_DATE("dateLienSuccession", 'YYYY-MM-DD')     AS "date_lien_succession"
FROM source
