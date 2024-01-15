WITH source AS (
    {{ stg_source_header('france_travail', 'agences') }}
),

final AS (
    SELECT
        _di_source_id                                                                                                                                        AS "_di_source_id",
        CURRENT_DATE                                                                                                                                         AS "date_maj",
        CASE WHEN data ->> 'dispositifADEDA' = 'true' THEN 'https://www.pole-emploi.fr/actualites/a-laffiche/2022/adeda-un-dispositif-pour-mieux-a.html' END AS "accessibilite",
        CAST(data #>> '{adressePrincipale,gpsLat}' AS FLOAT)                                                                                                 AS "latitude",
        CAST(data #>> '{adressePrincipale,gpsLon}' AS FLOAT)                                                                                                 AS "longitude",
        data #>> '{adressePrincipale,ligne4}'                                                                                                                AS "adresse",
        data #>> '{adressePrincipale,ligne3}'                                                                                                                AS "complement_adresse",
        data #>> '{adressePrincipale,communeImplantation}'                                                                                                   AS "code_insee",
        data #>> '{adressePrincipale,bureauDistributeur}'                                                                                                    AS "code_postal",
        data #>> '{contact,email}'                                                                                                                           AS "courriel",
        data #>> '{contact,telephonePublic}'                                                                                                                 AS "telephone",
        data ->> 'code'                                                                                                                                      AS "id",
        data ->> 'libelleEtendu'                                                                                                                             AS "nom",
        data ->> 'siret'                                                                                                                                     AS "siret",
        data ->> 'type'                                                                                                                                      AS "typologie"
    FROM source
)

SELECT * FROM final
