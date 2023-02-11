WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'etab_pub'
),

final AS (
    SELECT
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'adresse_courriel'))::TEXT[] AS "adresse_courriel",
        data ->> 'id'                                                                      AS "id",
        data ->> 'siret'                                                                   AS "siret",
        data ->> 'siren'                                                                   AS "siren",
        data ->> 'nom'                                                                     AS "nom",
        data ->> 'nom_commune'                                                             AS "nom_commune",
        data ->> 'code_postal'                                                             AS "code_postal",
        data ->> 'code_insee_commune'                                                      AS "code_insee_commune",
        data ->> 'numero_voie'                                                             AS "numero_voie",
        data ->> 'complement1'                                                             AS "complement1",
        data ->> 'longitude'                                                               AS "longitude",
        data ->> 'latitude'                                                                AS "latitude",
        data ->> 'type_service_local'                                                      AS "type_service_local",
        data ->> 'mission'                                                                 AS "mission",
        data ->> 'date_modification'                                                       AS "date_modification",
        data ->> 'partenaire'                                                              AS "partenaire"
    FROM source
)

SELECT * FROM final
