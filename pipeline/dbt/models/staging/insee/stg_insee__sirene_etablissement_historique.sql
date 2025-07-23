WITH source AS (
    SELECT * FROM {{ source('insee', 'sirene_etablissement_historique') }}
)

SELECT
    siret                                      AS "siret",
    "dateDebut"                                AS "date_debut",
    CASE "etatAdministratifEtablissement"
        WHEN 'A' THEN 'actif'
        WHEN 'F' THEN 'fermé'
    END                                        AS "etat_administratif_etablissement",
    "changementEtatAdministratifEtablissement" AS "changement_etat_administratif_etablissement"
FROM source
