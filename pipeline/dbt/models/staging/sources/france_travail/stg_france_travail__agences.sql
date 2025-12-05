WITH source AS (
    {{ stg_source_header('france_travail', 'agences') }}),

final AS (
    SELECT
        CAST(data ->> 'dispositifADEDA' AS BOOLEAN)                AS "dispositif_adeda",
        CAST(data -> 'adressePrincipale' ->> 'gpsLat' AS FLOAT)    AS "adresse_principale__gps_lat",
        CAST(data -> 'adressePrincipale' ->> 'gpsLon' AS FLOAT)    AS "adresse_principale__gps_lon",
        NULLIF(TRIM(data -> 'adressePrincipale' ->> 'ligne4'), '') AS "adresse_principale__ligne_4",
        NULLIF(TRIM(data -> 'adressePrincipale' ->> 'ligne3'), '') AS "adresse_principale__ligne_3",
        data -> 'adressePrincipale' ->> 'communeImplantation'      AS "adresse_principale__commune_implantation",
        data -> 'adressePrincipale' ->> 'bureauDistributeur'       AS "adresse_principale__bureau_distributeur",
        NULLIF(TRIM(data -> 'contact' ->> 'email'), '')            AS "contact__email",
        NULLIF(TRIM(data -> 'contact' ->> 'telephonePublic'), '')  AS "contact__telephone_public",
        data -> 'horaires'                                         AS "horaires",
        data ->> 'code'                                            AS "code",
        NULLIF(TRIM(data ->> 'libelleEtendu'), '')                 AS "libelle_etendu",
        NULLIF(TRIM(data ->> 'siret'), '')                         AS "siret",
        data ->> 'type'                                            AS "type"
    FROM source
)

SELECT * FROM final
