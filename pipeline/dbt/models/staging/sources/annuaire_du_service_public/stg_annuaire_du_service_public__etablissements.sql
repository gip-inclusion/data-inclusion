WITH source AS (
    {{ stg_source_header('annuaire_du_service_public', 'etablissements') }}),

final AS (
    SELECT
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'adresse_courriel')) AS TEXT [])                   AS "adresse_courriel",
        data ->> 'id'                                                                                                 AS "id",
        data ->> 'siret'                                                                                              AS "siret",
        data ->> 'siren'                                                                                              AS "siren",
        data ->> 'nom'                                                                                                AS "nom",
        data #>> '{adresse,0,nom_commune}'                                                                            AS "commune",
        data #>> '{adresse,0,code_postal}'                                                                            AS "code_postal",
        data #>> '{adresse,0,numero_voie}'                                                                            AS "adresse",
        data #>> '{adresse,0,complement1}'                                                                            AS "complement1",
        CASE WHEN (data #>> '{adresse,0,latitude}') != '' THEN CAST((data #>> '{adresse,0,latitude}') AS FLOAT) END   AS "latitude",
        CASE WHEN (data #>> '{adresse,0,longitude}') != '' THEN CAST((data #>> '{adresse,0,longitude}') AS FLOAT) END AS "longitude",
        data #>> '{pivot,0,code_insee_commune,0}'                                                                     AS "code_insee",
        data #>> '{pivot,0,type_service_local}'                                                                       AS "type_service_local",
        data #>> '{telephone,0,valeur}'                                                                               AS "telephone",
        data #>> '{site_internet,0,valeur}'                                                                           AS "site_internet",
        data ->> 'mission'                                                                                            AS "mission",
        data ->> 'date_modification'                                                                                  AS "date_maj",
        data ->> 'partenaire'                                                                                         AS "partenaire"
    FROM source
)

SELECT * FROM final
