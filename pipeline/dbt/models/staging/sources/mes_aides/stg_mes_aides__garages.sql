WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}
),

final AS (
    SELECT
        _di_source_id                                                                                      AS "_di_source_id",
        data #>> '{fields,Adresse}'                                                                        AS "adresse",
        data #>> '{fields,Code INSEE}'                                                                     AS "code_insee",
        data #>> '{fields,Code Postal}'                                                                    AS "code_postal",
        CAST((data #>> '{fields,Créé le}') AS DATE)                                                        AS "cree_le",
        CASE
            WHEN data #>> '{fields,Critères d''éligibilité}' IS NULL THEN NULL
            -- if bullet points list, split to array
            WHEN data #>> '{fields,Critères d''éligibilité}' ~ '^\W '
                THEN
                    ARRAY_REMOVE(
                        ARRAY(
                            SELECT (REGEXP_MATCHES(data #>> '{fields,Critères d''éligibilité}', '^\W (.*?)( )*?$', 'gn'))[1]
                        ),
                        NULL
                    )
            -- else use the whole field
            ELSE ARRAY[data #>> '{fields,Critères d''éligibilité}']
        END                                                                                                AS "criteres_eligibilite",
        data #>> '{fields,Critères d''éligibilité}'                                                        AS "criteres_eligibilite_raw",
        data #>> '{fields,Département Nom}'                                                                AS "departement_nom",
        TRIM(data #>> '{fields,Email}')                                                                    AS "email",
        CAST((data #>> '{fields,En Ligne}') AS BOOLEAN)                                                    AS "en_ligne",
        data #>> '{fields,ID}'                                                                             AS "id",
        -- some rows are formatted as `LAT, LAT`... use first value
        CAST((SPLIT_PART(data #>> '{fields,Latitude}', ',', 1)) AS FLOAT)                                  AS "latitude",
        CAST((SPLIT_PART(data #>> '{fields,Longitude}', ',', 1)) AS FLOAT)                                 AS "longitude",
        CAST((data #>> '{fields,Modifié le}') AS DATE)                                                     AS "modifie_le",
        data #>> '{fields,Nom}'                                                                            AS "nom",
        data #>> '{fields,Partenaire Nom}'                                                                 AS "partenaire_nom",
        data #>> '{fields,Région Nom}'                                                                     AS "region_nom",
        ARRAY(SELECT e.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data #> '{fields,Services}') AS e)          AS "services",
        -- FIXME(hlecuyer) : Remove this when mes_aides has fixed the issue
        REPLACE(data #>> '{fields,SIRET}', ' ', '')                                                        AS "siret",
        data #>> '{fields,Téléphone}'                                                                      AS "telephone",
        data #>> '{fields,Type}'                                                                           AS "type",
        ARRAY(SELECT e.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data #> '{fields,Types de véhicule}') AS e) AS "types_de_vehicule",
        data #>> '{fields,Url}'                                                                            AS "url",
        data #>> '{fields,Ville Nom}'                                                                      AS "ville_nom"
    FROM source
)

SELECT * FROM final
