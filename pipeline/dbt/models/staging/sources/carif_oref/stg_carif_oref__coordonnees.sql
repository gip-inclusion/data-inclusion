WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

all_coordonnees AS (
    SELECT contacts_formateurs.data -> 'coordonnees' AS data_
    FROM
        source,  -- noqa: structure.unused_join
        JSONB_PATH_QUERY(source.data, '$.action[*].organisme\-formateur[*]') AS organismes_formateurs (data),  -- noqa: structure.unused_join
        JSONB_PATH_QUERY(organismes_formateurs.data, '$.contact\-formateur[*]') AS contacts_formateurs (data)
),

final AS (
    SELECT DISTINCT ON (1)
        CAST(MD5(CAST(all_coordonnees.data_ AS TEXT)) AS TEXT) AS "hash_",
        NULLIF(TRIM(all_coordonnees.data_ ->> 'courriel'), '') AS "courriel",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY(
                    SELECT NULLIF(TRIM(x.urlweb), '')
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(all_coordonnees.data_ -> 'web' -> 'urlweb') AS x (urlweb)
                ),
                NULL
            ),
            '{}'
        )                                                      AS "web",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY(
                    SELECT NULLIF(TRIM(x.numtel), '')
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(all_coordonnees.data_ -> 'telfixe' -> 'numtel') AS x (numtel)
                ),
                NULL
            ),
            '{}'
        )                                                      AS "telfixe",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY(
                    SELECT NULLIF(TRIM(x.numtel), '')
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(all_coordonnees.data_ -> 'portable' -> 'numtel') AS x (numtel)
                ),
                NULL
            ),
            '{}'
        )                                                      AS "portable",
        CAST(MD5(all_coordonnees.data_ ->> 'adresse') AS TEXT) AS "hash_adresse"
    FROM all_coordonnees
    WHERE all_coordonnees.data_ IS NOT NULL
)

SELECT * FROM final
