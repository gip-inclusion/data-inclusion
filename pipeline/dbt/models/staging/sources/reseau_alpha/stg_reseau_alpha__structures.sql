WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

adresses AS (
    SELECT
        -- extracted from cartographie.json
        source.data ->> 'id'                                                          AS "structure_id",
        adresses.data ->> 'ville'                                                     AS "adresses__ville",
        CAST(adresses.data ->> 'latitude' AS FLOAT)                                   AS "adresses__latitude",
        CAST(adresses.data ->> 'longitude' AS FLOAT)                                  AS "adresses__longitude",
        adresses.data ->> 'codePostal'                                                AS "adresses__code_postal",
        TRIM(SUBSTRING(source.data ->> 'content__adresse' FROM '^(.+)\s\d{5} - .+$')) AS "content__adresse"
    FROM
        source,
        LATERAL (SELECT * FROM JSONB_PATH_QUERY(source.data, '$.adresses[*]')) AS adresses (data)
    WHERE
        -- a minority of structures have more than one addresses, which is not managed by
        -- the data·inclusion schema. Skip these addresses.
        JSONB_ARRAY_LENGTH(source.data -> 'adresses') = 1
),

final AS (
    SELECT
        adresses.adresses__ville                                                                               AS "adresses__ville",
        adresses.adresses__latitude                                                                            AS "adresses__latitude",
        adresses.adresses__longitude                                                                           AS "adresses__longitude",
        adresses.adresses__code_postal                                                                         AS "adresses__code_postal",
        COALESCE(NULLIF(TRIM(SPLIT_PART(adresses.content__adresse, E'\n', 2)), ''), adresses.content__adresse) AS "content__adresse",
        NULLIF(TRIM(SPLIT_PART(adresses.content__adresse, E'\n', 3)), '')                                      AS "content__complement_adresse",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'activitesFormation')) AS TEXT [])   AS "activites_formation",
        source.data ->> 'id'                                                                                   AS "id",
        source.data ->> 'nom'                                                                                  AS "nom",
        source.data ->> 'url'                                                                                  AS "url",
        source.data ->> 'logo'                                                                                 AS "logo",
        source.data ->> 'type'                                                                                 AS "type",
        source.data ->> 'description'                                                                          AS "description",
        TO_DATE(
            SUBSTRING(source.data ->> 'content__date_maj' FROM 'Date de la dernière modification : (.*)'),
            'DD TMmonth YYYY'
        )                                                                                                      AS "content__date_maj",
        TRIM(source.data ->> 'content__telephone')                                                             AS "content__telephone",
        TRIM(source.data ->> 'content__courriel')                                                              AS "content__courriel",
        TRIM(source.data ->> 'content__site_web')                                                              AS "content__site_web"
    FROM source
    LEFT JOIN adresses ON source.data ->> 'id' = adresses.structure_id
)

SELECT * FROM final
