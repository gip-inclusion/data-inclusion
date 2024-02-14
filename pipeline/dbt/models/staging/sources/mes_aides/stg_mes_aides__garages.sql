WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}
),

final AS (
    SELECT
        _di_source_id                                              AS "_di_source_id",
        (data #>> '{fields,Créé le}')::TIMESTAMP WITH TIME ZONE    AS "cree_le",
        (data #>> '{fields,Modifié le}')::TIMESTAMP WITH TIME ZONE AS "modifie_le",
        (data #>> '{fields,En Ligne}')::BOOLEAN                    AS "en_ligne",
        -- some rows are formatted as `LAT, LAT`... use first value
        SPLIT_PART(data #>> '{fields,Longitude}', ',', 1)::FLOAT   AS "ville_longitude",
        SPLIT_PART(data #>> '{fields,Latitude}', ',', 1)::FLOAT    AS "ville_latitude",
        data #>> '{fields,ID}'                                     AS "id",
        data #>> '{fields,Nom}'                                    AS "nom",
        data #>> '{fields,Ville Nom}'                              AS "ville_nom",
        data #>> '{fields,Code Postal}'                            AS "code_postal",
        data #>> '{fields,Code INSEE}'                             AS "code_insee",
        data #>> '{fields,Adresse}'                                AS "adresse",
        data #>> '{fields,Téléphone}'                              AS "telephone",
        data #>> '{fields,SIRET}'                                  AS "siret",
        data #>> '{fields,Email}'                                  AS "email",
        data #>> '{fields,Url}'                                    AS "url",
        data #>> '{fields,Département Nom}'                        AS "departement_nom",
        data #>> '{fields,Région Nom}'                             AS "region_nom",
        data #>> '{fields,Type}'                                   AS "type"
    FROM source
)

SELECT * FROM final
