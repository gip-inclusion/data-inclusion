WITH source AS (
    SELECT * FROM {{ source('reseau_alpha', 'formations') }}
),

final AS (
    SELECT
        _di_source_id                                    AS "_di_source_id",

        -- extracted from cartographie.json
        data ->> 'id'                                    AS "id",
        data ->> 'structure_id'                          AS "structure_id",
        data ->> 'nom'                                   AS "nom",
        data ->> 'url'                                   AS "url",

        -- extracted from html content
        TRIM(data ->> 'content__contenu_et_objectifs')   AS "content__contenu_et_objectifs",
        TRIM(data ->> 'content__public_attendu')         AS "content__public_attendu",
        TRIM(data ->> 'content__inscription')            AS "content__inscription",
        TRIM(data ->> 'content__contact_prenom_nom')     AS "content__contact_prenom_nom",
        TRIM(data ->> 'content__telephone')              AS "content__telephone",
        TRIM(data ->> 'content__courriel')               AS "content__courriel",
        TRIM(data ->> 'content__informations_pratiques') AS "content__informations_pratiques"
    FROM source
)

SELECT * FROM final
