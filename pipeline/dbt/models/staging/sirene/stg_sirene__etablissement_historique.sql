WITH source AS (
    SELECT * FROM {{ source('sirene', 'etablissement_historique') }}
)

SELECT
    siret                                                       AS "siret",
    TO_DATE("dateDebut", 'YYYY-MM-DD')                          AS "date_debut",
    CASE "etatAdministratifEtablissement"
        WHEN 'A' THEN 'actif'
        WHEN 'F' THEN 'fermé'
    END                                                         AS "etat_administratif_etablissement",
    CAST("changementEtatAdministratifEtablissement" AS BOOLEAN) AS "changement_etat_administratif_etablissement"
FROM source
