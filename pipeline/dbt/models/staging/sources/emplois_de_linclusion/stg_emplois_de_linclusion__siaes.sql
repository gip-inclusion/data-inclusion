WITH source AS (
    {{ stg_source_header('emplois_de_linclusion', 'siaes') }}),

final AS (
    SELECT
        source.data ->> 'id'                                  AS "id",
        source.data ->> 'nom'                                 AS "nom",
        CAST(source.data ->> 'date_maj' AS DATE)              AS "date_maj",
        source.data ->> 'kind'                                AS "kind",
        NULLIF(source.data ->> 'commune', '')                 AS "commune",
        NULLIF(source.data ->> 'siret', '')                   AS "siret",
        NULLIF(source.data ->> 'description', '')             AS "description",
        NULLIF(source.data ->> 'adresse', '')                 AS "adresse",
        NULLIF(source.data ->> 'complement_adresse', '')      AS "complement_adresse",
        NULLIF(source.data ->> 'code_postal', '')             AS "code_postal",
        NULLIF(source.data ->> 'site_web', '')                AS "site_web",
        NULLIF(CAST(source.data ->> 'longitude' AS FLOAT), 0) AS "longitude",
        NULLIF(CAST(source.data ->> 'latitude' AS FLOAT), 0)  AS "latitude",
        NULLIF(source.data ->> 'lien_source', '')             AS "lien_source",
        NULLIF(source.data ->> 'telephone', '')               AS "telephone",
        NULLIF(source.data ->> 'courriel', '')                AS "courriel"
    FROM source
)

SELECT * FROM final
