WITH source AS (
    SELECT * FROM {{ source('reseau_alpha', 'formations') }}
),

adresses AS (
    SELECT
        -- extracted from cartographie.json
        source.data ->> 'id'                                                          AS "formation_id",
        adresses.data ->> 'ville'                                                     AS "adresses__ville",
        CAST(adresses.data ->> 'latitude' AS FLOAT)                                   AS "adresses__latitude",
        CAST(adresses.data ->> 'longitude' AS FLOAT)                                  AS "adresses__longitude",
        adresses.data ->> 'codePostal'                                                AS "adresses__code_postal",
        TRIM(SUBSTRING(source.data ->> 'content__adresse' FROM '^(.+)\s\d{5} - .+$')) AS "content__adresse",
        TRIM(source.data ->> 'content__horaires')                                     AS "content__horaires"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.adresses[*]')) AS adresses (data)
    WHERE
        -- a minority of formations have more than one addresses, which is not managed by
        -- the data·inclusion schema. Skip these addresses.
        JSONB_ARRAY_LENGTH(source.data -> 'adresses') = 1
),

final AS (
    SELECT
        source._di_source_id                                    AS "_di_source_id",
        adresses.adresses__ville                                AS "adresses__ville",
        adresses.adresses__latitude                             AS "adresses__latitude",
        adresses.adresses__longitude                            AS "adresses__longitude",
        adresses.adresses__code_postal                          AS "adresses__code_postal",
        adresses.content__adresse                               AS "content__adresse",
        adresses.content__horaires                              AS "content__horaires",
        source.data ->> 'id'                                    AS "id",
        source.data ->> 'structure_id'                          AS "structure_id",
        source.data ->> 'nom'                                   AS "nom",
        source.data ->> 'url'                                   AS "url",
        source.data ->> 'activite'                              AS "activite",
        TO_DATE(
            SUBSTRING(
                (
                    CASE
                        -- TODO: remove this after making fr_FR locale available
                        WHEN (source.data ->> 'content__date_maj') ~ 'janvier' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'janvier', '01')
                        WHEN (source.data ->> 'content__date_maj') ~ 'février' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'février', '02')
                        WHEN (source.data ->> 'content__date_maj') ~ 'mars' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'mars', '03')
                        WHEN (source.data ->> 'content__date_maj') ~ 'avril' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'avril', '04')
                        WHEN (source.data ->> 'content__date_maj') ~ 'mai' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'mai', '05')
                        WHEN (source.data ->> 'content__date_maj') ~ 'juin' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'juin', '06')
                        WHEN (source.data ->> 'content__date_maj') ~ 'juillet' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'juillet', '07')
                        WHEN (source.data ->> 'content__date_maj') ~ 'août' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'août', '08')
                        WHEN (source.data ->> 'content__date_maj') ~ 'septembre' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'septembre', '09')
                        WHEN (source.data ->> 'content__date_maj') ~ 'octobre' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'octobre', '10')
                        WHEN (source.data ->> 'content__date_maj') ~ 'novembre' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'novembre', '11')
                        WHEN (source.data ->> 'content__date_maj') ~ 'décembre' THEN REGEXP_REPLACE(source.data ->> 'content__date_maj', 'décembre', '12')
                    END
                ) FROM 'Date de la dernière modification : (.*)'
            ),
            'DD MM YYYY'
        )                                                       AS "content__date_maj",
        TRIM(source.data ->> 'content__contenu_et_objectifs')   AS "content__contenu_et_objectifs",
        TRIM(source.data ->> 'content__public_attendu')         AS "content__public_attendu",
        TRIM(source.data ->> 'content__inscription')            AS "content__inscription",
        TRIM(source.data ->> 'content__contact_prenom_nom')     AS "content__contact_prenom_nom",
        TRIM(source.data ->> 'content__telephone')              AS "content__telephone",
        TRIM(source.data ->> 'content__courriel')               AS "content__courriel",
        TRIM(source.data ->> 'content__informations_pratiques') AS "content__informations_pratiques"
    FROM source
    LEFT JOIN adresses ON source.data ->> 'id' = adresses.formation_id
)

SELECT * FROM final
