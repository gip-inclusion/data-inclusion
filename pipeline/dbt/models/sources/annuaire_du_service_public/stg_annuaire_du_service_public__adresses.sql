WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'etab_pub'
),

final AS (
    SELECT
        source.data ->> 'id'                                                                            AS "etablissement_id",
        adresses.data ->> 'code_insee_commune'                                                          AS "code_insee_commune",
        adresses.data ->> 'type_service_local'                                                          AS "type_service_local",
        adresses.data ->> 'pays'                                                                        AS "pays",
        CASE WHEN (adresses.data ->> 'latitude') != '' THEN (adresses.data ->> 'latitude')::FLOAT END   AS "latitude",
        CASE WHEN (adresses.data ->> 'longitude') != '' THEN (adresses.data ->> 'longitude')::FLOAT END AS "longitude",
        adresses.data ->> 'continent'                                                                   AS "continent",
        adresses.data ->> 'code_postal'                                                                 AS "code_postal",
        adresses.data ->> 'complement1'                                                                 AS "complement1",
        adresses.data ->> 'complement2'                                                                 AS "complement2",
        adresses.data ->> 'numero_voie'                                                                 AS "numero_voie",
        adresses.data ->> 'type_adresse'                                                                AS "type_adresse",
        adresses.data ->> 'accessibilite'                                                               AS "accessibilite",
        adresses.data ->> 'note_accessibilite'                                                          AS "note_accessibilite",
        adresses.data ->> 'service_distribution'                                                        AS "service_distribution"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.adresse[*]')) AS adresses(data)
)

SELECT * FROM final
