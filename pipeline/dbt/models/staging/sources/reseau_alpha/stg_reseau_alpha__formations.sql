WITH source AS (
    {{ stg_source_header('reseau_alpha', 'formations') }}
),

adresses AS (
    SELECT
        -- extracted from cartographie.json
        source.data ->> 'id'                                                                                       AS "formation_id",
        adresses.data ->> 'ville'                                                                                  AS "adresses__ville",
        CAST(adresses.data ->> 'latitude' AS FLOAT)                                                                AS "adresses__latitude",
        CAST(adresses.data ->> 'longitude' AS FLOAT)                                                               AS "adresses__longitude",
        adresses.data ->> 'codePostal'                                                                             AS "adresses__code_postal",
        TRIM(SUBSTRING(source.data ->> 'content__lieux_et_horaires_formation__adresse' FROM '^(.+)\s\d{5} - .+$')) AS "content__lieux_et_horaires_formation__adresse",
        TRIM(source.data ->> 'content__lieux_et_horaires_formation__horaires')                                     AS "content__lieux_et_horaires_formation__horaires"
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
        source._di_source_id                                                     AS "_di_source_id",
        adresses.adresses__ville                                                 AS "adresses__ville",
        adresses.adresses__latitude                                              AS "adresses__latitude",
        adresses.adresses__longitude                                             AS "adresses__longitude",
        adresses.adresses__code_postal                                           AS "adresses__code_postal",
        adresses.content__lieux_et_horaires_formation__adresse                   AS "content__lieux_et_horaires_formation__adresse",
        adresses.content__lieux_et_horaires_formation__horaires                  AS "content__lieux_et_horaires_formation__horaires",
        source.data ->> 'id'                                                     AS "id",
        source.data ->> 'structure_id'                                           AS "structure_id",
        source.data ->> 'nom'                                                    AS "nom",
        source.data ->> 'url'                                                    AS "url",
        source.data ->> 'activite'                                               AS "activite",
        TO_DATE(
            SUBSTRING(source.data ->> 'content__date_maj' FROM 'Date de la dernière modification : (.*)'),
            'DD TMmonth YYYY'
        )                                                                        AS "content__date_maj",
        TRIM(source.data ->> 'content__contenu_et_objectifs__titre')             AS "content__contenu_et_objectifs__titre",
        TRIM(source.data ->> 'content__contenu_et_objectifs__objectifs')         AS "content__contenu_et_objectifs__objectifs",
        TRIM(source.data ->> 'content__contenu_et_objectifs__niveau')            AS "content__contenu_et_objectifs__niveau",
        TRIM(source.data ->> 'content__public_attendu__niveau')                  AS "content__public_attendu__niveau",
        TRIM(source.data ->> 'content__public_attendu__competences')             AS "content__public_attendu__competences",
        TRIM(source.data ->> 'content__public_attendu__type_de_public')          AS "content__public_attendu__type_de_public",
        TRIM(source.data ->> 'content__inscription__informations_en_ligne')      AS "content__inscription__informations_en_ligne",
        TRIM(source.data ->> 'content__inscription__places')                     AS "content__inscription__places",
        TRIM(source.data ->> 'content__inscription__entree_sortie')              AS "content__inscription__entree_sortie",
        TRIM(source.data ->> 'content__contact_inscription__adresse')            AS "content__contact_inscription__adresse",
        TRIM(source.data ->> 'content__contact_inscription__contact')            AS "content__contact_inscription__contact",
        TRIM(source.data ->> 'content__contact_inscription__telephone')          AS "content__contact_inscription__telephone",
        TRIM(source.data ->> 'content__contact_inscription__courriel')           AS "content__contact_inscription__courriel",
        TRIM(source.data ->> 'content__informations_pratiques__etendue')         AS "content__informations_pratiques__etendue",
        TRIM(source.data ->> 'content__informations_pratiques__volume')          AS "content__informations_pratiques__volume",
        TRIM(source.data ->> 'content__informations_pratiques__cout')            AS "content__informations_pratiques__cout",
        TRIM(source.data ->> 'content__informations_pratiques__prise_en_charge') AS "content__informations_pratiques__prise_en_charge",
        TRIM(source.data ->> 'content__informations_pratiques__remuneration')    AS "content__informations_pratiques__remuneration",
        TRIM(source.data ->> 'content__informations_pratiques__garde')           AS "content__informations_pratiques__garde"
    FROM source
    LEFT JOIN adresses ON source.data ->> 'id' = adresses.formation_id
)

SELECT * FROM final
