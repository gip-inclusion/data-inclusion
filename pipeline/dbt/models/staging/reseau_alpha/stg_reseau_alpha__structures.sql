WITH source AS (
    SELECT * FROM {{ source('reseau_alpha', 'structures') }}
),

adresses AS (
    SELECT
        -- extracted from cartographie.json
        source.data ->> 'id'           AS "structure_id",
        adresses.data ->> 'ville'      AS "adresses_ville",
        adresses.data ->> 'latitude'   AS "adresses_latitude",
        adresses.data ->> 'longitude'  AS "adresses_longitude",
        adresses.data ->> 'codePostal' AS "adresses_code_postal"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.adresses[*]')) AS adresses (data)
    WHERE
        JSONB_ARRAY_LENGTH(source.data -> 'adresses') = 1
),

final AS (
    SELECT
        adresses.*,
        source._di_source_id                                                                                 AS "_di_source_id",

        -- extracted from cartographie.json
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'activitesFormation'))::TEXT []         AS "activites_formation",
        source.data ->> 'id'                                                                                 AS "id",
        source.data ->> 'nom'                                                                                AS "nom",
        source.data ->> 'url'                                                                                AS "url",
        source.data ->> 'logo'                                                                               AS "logo",
        source.data ->> 'type'                                                                               AS "type",
        source.data ->> 'description'                                                                        AS "description",

        -- extracted from html content
        TRIM(SUBSTRING(source.data ->> 'content__date_maj' FROM 'Date de la dernière modification : (.*)')) AS "content__date_maj",  -- TODO: use locale
        TRIM(source.data ->> 'content__telephone')                                                           AS "content__telephone",
        TRIM(source.data ->> 'content__courriel')                                                            AS "content__courriel",
        TRIM(source.data ->> 'content__adresse')                                                             AS "content__adresse",
        TRIM(source.data ->> 'content__site_web')                                                            AS "content__site_web"
    FROM source
    LEFT JOIN adresses ON source.data ->> 'id' = adresses.structure_id
)

SELECT * FROM final
