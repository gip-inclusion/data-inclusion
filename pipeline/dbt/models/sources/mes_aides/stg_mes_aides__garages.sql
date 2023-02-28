WITH source AS (
    SELECT * FROM {{ source('mes_aides', 'garages') }}
),

final AS (
    SELECT
        (data #>> '{fields,Créé le}')::TIMESTAMP WITH TIME ZONE        AS "cree_le",
        (data #>> '{fields,Modifié le}')::TIMESTAMP WITH TIME ZONE     AS "modifie_le",
        (data #>> '{fields,En Ligne}')::BOOLEAN                        AS "en_ligne",
        SPLIT_PART(data #>> '{fields,Ville Longitude}', ',', 1)::FLOAT AS "ville_longitude",
        SPLIT_PART(data #>> '{fields,Ville Latitude}', ',', 1)::FLOAT  AS "ville_latitude",
        data #>> '{fields,ID}'                                         AS "id",
        data #>> '{fields,Nom}'                                        AS "nom",
        data #>> '{fields,Ville Nom}'                                  AS "ville_nom",
        data #>> '{fields,Code Postal}'                                AS "code_postal",
        -- some rows are formatted as `LAT, LAT`... use first value
        data #>> '{fields,Code INSEE}'                                 AS "code_insee",
        -- some rows are formatted as `LON, LON`... use first value
        data #>> '{fields,Adresse}'                                    AS "adresse",
        data #>> '{fields,Téléphone}'                                  AS "telephone",
        data #>> '{fields,Email}'                                      AS "email",
        data #>> '{fields,Url}'                                        AS "url",
        data #>> '{fields,Département Nom}'                            AS "departement_nom",
        data #>> '{fields,Région Nom}'                                 AS "region_nom",
        data #>> '{fields,Type}'                                       AS "type"
    FROM source
)

SELECT * FROM final
