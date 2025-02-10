WITH source AS (
    {{ stg_source_header('carif_oref', 'formations') }}
),

arrondissements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__arrondissements') }}
),

all_adresses AS (
    SELECT contacts_formateurs.data -> 'coordonnees' -> 'adresse' AS data
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.action[*].organisme\-formateur[*]')    AS organismes_formateurs (data),
        JSONB_PATH_QUERY(organismes_formateurs.data, '$.contact\-formateur[*]') AS contacts_formateurs (data)
),

final AS (
    SELECT
        DISTINCT ON (hash_)
        CAST(MD5(CAST (all_adresses.data AS TEXT)) AS TEXT) AS "hash_",
        all_adresses.data ->> 'denomination'          AS "denomination",
        ARRAY_REMOVE(
            ARRAY(
                SELECT NULLIF(TRIM(ligne), '')
                FROM JSONB_ARRAY_ELEMENTS_TEXT(all_adresses.data -> 'ligne') AS ligne
            ),
            NULL
        )                                AS "ligne",
        NULLIF(TRIM(all_adresses.data -> 'ligne' ->> 0), '') AS "ligne__1",
        NULLIF(TRIM(all_adresses.data ->> 'codepostal'), '')            AS "codepostal",
        NULLIF(TRIM(all_adresses.data ->> 'ville'), '')                 AS "ville",
        NULLIF(TRIM(all_adresses.data ->> 'departement'), '')           AS "departement",
        COALESCE(arrondissements.code_commune, NULLIF(TRIM(all_adresses.data ->> 'code-INSEE-commune'), ''))    AS "code_insee_commune",
        arrondissements.code    AS "code_insee_arrondissement",
        NULLIF(TRIM(all_adresses.data ->> 'region'), '')                AS "region",
        NULLIF(TRIM(all_adresses.data ->> 'pays'), '')                  AS "pays",
        CAST(all_adresses.data -> 'geolocalisation' ->> 'latitude' AS FLOAT)       AS "geolocalisation__latitude",
        CAST(all_adresses.data -> 'geolocalisation' ->> 'longitude' AS FLOAT)       AS "geolocalisation__longitude"
    FROM
        all_adresses
        LEFT JOIN arrondissements ON NULLIF(TRIM(all_adresses.data ->> 'code-INSEE-commune'), '') = arrondissements.code
    WHERE
        data IS NOT NULL
)

SELECT * FROM final
