WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}
),

final AS (
    SELECT
        _di_source_id                                              AS "_di_source_id",
        (data #>> '{fields,Ville Longitude}')::FLOAT               AS "ville_longitude",
        (data #>> '{fields,Ville Latitude}')::FLOAT                AS "ville_latitude",
        (data #>> '{fields,Modifié le}')::TIMESTAMP WITH TIME ZONE AS "modifie_le",
        data #>> '{fields,ID}'                                     AS "id",
        data #>> '{fields,Nom}'                                    AS "nom",
        data #>> '{fields,Ville Nom}'                              AS "ville_nom",
        data #>> '{fields,Code Postal}'                            AS "code_postal",
        data #>> '{fields,Adresse}'                                AS "adresse",
        data #>> '{fields,Téléphone}'                              AS "telephone",
        data #>> '{fields,Email}'                                  AS "email",
        data #>> '{fields,Url}'                                    AS "url"
    FROM source
)

SELECT * FROM final
