WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

arrondissements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__arrondissements') }}
),

all_adresses AS (
    SELECT contacts_formateurs.data -> 'coordonnees' -> 'adresse' AS data_
    FROM
        source,  -- noqa: structure.unused_join
        JSONB_PATH_QUERY(source.data, '$.action[*].organisme\-formateur[*]') AS organismes_formateurs (data),  -- noqa: structure.unused_join
        JSONB_PATH_QUERY(organismes_formateurs.data, '$.contact\-formateur[*]') AS contacts_formateurs (data)
    UNION ALL
    (
        SELECT DISTINCT ON (1) lieux_de_formation.data -> 'coordonnees' -> 'adresse' AS data_
        FROM
            source,  -- noqa: structure.unused_join
            JSONB_PATH_QUERY(source.data, '$.action[*].lieu\-de\-formation[*]') AS lieux_de_formation (data)
        ORDER BY
            lieux_de_formation.data -> 'coordonnees' -> 'adresse',
            (lieux_de_formation.data ->> '@tag') = 'principal' DESC
    )
    UNION ALL
    (
        SELECT contacts_formations.data -> 'coordonnees' -> 'adresse' AS data_
        FROM
            source,  -- noqa: structure.unused_join
            JSONB_PATH_QUERY(source.data, '$.contact\-formation[*]') AS contacts_formations (data)
    )
),

final AS (
    SELECT DISTINCT ON (1)
        CAST(MD5(CAST(all_adresses.data_ AS TEXT)) AS TEXT)                                                   AS "hash_",
        all_adresses.data_ ->> 'denomination'                                                                 AS "denomination",
        ARRAY_REMOVE(
            ARRAY(
                SELECT NULLIF(TRIM(x.ligne), '')
                FROM JSONB_ARRAY_ELEMENTS_TEXT(all_adresses.data_ -> 'ligne') AS x (ligne)
            ),
            NULL
        )                                                                                                     AS "ligne",
        NULLIF(TRIM(all_adresses.data_ -> 'ligne' ->> 0), '')                                                 AS "ligne__1",
        NULLIF(TRIM(all_adresses.data_ ->> 'codepostal'), '')                                                 AS "codepostal",
        NULLIF(TRIM(all_adresses.data_ ->> 'ville'), '')                                                      AS "ville",
        NULLIF(TRIM(all_adresses.data_ ->> 'departement'), '')                                                AS "departement",
        COALESCE(arrondissements.code_commune, NULLIF(TRIM(all_adresses.data_ ->> 'code-INSEE-commune'), '')) AS "code_insee_commune",
        arrondissements.code                                                                                  AS "code_insee_arrondissement",
        NULLIF(TRIM(all_adresses.data_ ->> 'region'), '')                                                     AS "region",
        NULLIF(TRIM(all_adresses.data_ ->> 'pays'), '')                                                       AS "pays",
        CAST(all_adresses.data_ -> 'geolocalisation' ->> 'latitude' AS FLOAT)                                 AS "geolocalisation__latitude",
        CAST(all_adresses.data_ -> 'geolocalisation' ->> 'longitude' AS FLOAT)                                AS "geolocalisation__longitude"
    FROM all_adresses
    LEFT JOIN arrondissements ON NULLIF(TRIM(all_adresses.data_ ->> 'code-INSEE-commune'), '') = arrondissements.code
    WHERE all_adresses.data_ IS NOT NULL
)

SELECT * FROM final
