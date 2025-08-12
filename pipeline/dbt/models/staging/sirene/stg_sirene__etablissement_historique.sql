WITH source AS (
    SELECT * FROM {{ source('sirene', 'etablissement_historique') }}
)

SELECT
    LPAD(siret, 14, '0')                                        AS "siret",
    TO_DATE("dateDebut", 'YYYY-MM-DD')                          AS "date_debut",
    CASE "etatAdministratifEtablissement"
        WHEN 'A' THEN 'actif'
        WHEN 'F' THEN 'fermé'
    END                                                         AS "etat_administratif_etablissement",
    CAST("changementEtatAdministratifEtablissement" AS BOOLEAN) AS "changement_etat_administratif_etablissement"
FROM source
