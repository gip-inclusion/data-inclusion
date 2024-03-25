WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}
),

final AS (
    SELECT
        _di_source_id                                                    AS "_di_source_id",
        data #>> '{fields,Adresse}'                                      AS "adresse",
        data #>> '{fields,Code INSEE}'                                   AS "code_insee",
        data #>> '{fields,Code Postal}'                                  AS "code_postal",
        CAST(data #>> '{fields,Créé le}' AS DATE)                        AS "cree_le",
        data #>> '{fields,Département Nom}'                              AS "departement_nom",
        data #>> '{fields,Email}'                                        AS "email",
        CAST(data #>> '{fields,En Ligne}' AS BOOLEAN)                    AS "en_ligne",
        data #>> '{fields,ID}'                                           AS "id",
        -- some rows are formatted as `LAT, LAT`... use first value
        CAST(SPLIT_PART(data #>> '{fields,Latitude}', ',', 1) AS FLOAT)  AS "latitude",
        CAST(SPLIT_PART(data #>> '{fields,Longitude}', ',', 1) AS FLOAT) AS "longitude",
        CAST(data #>> '{fields,Modifié le}' AS DATE)                     AS "modifie_le",
        data #>> '{fields,Nom}'                                          AS "nom",
        data #>> '{fields,Région Nom}'                                   AS "region_nom",
        data #>> '{fields,SIRET}'                                        AS "siret",
        data #>> '{fields,Téléphone}'                                    AS "telephone",
        data #>> '{fields,Type}'                                         AS "type",
        data #>> '{fields,Url}'                                          AS "url",
        data #>> '{fields,Ville Nom}'                                    AS "ville_nom"
    FROM source
)

SELECT * FROM final
