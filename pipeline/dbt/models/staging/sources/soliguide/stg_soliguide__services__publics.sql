WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

administrative AS (
    SELECT
        source.data ->> 'lieu_id'                                                       AS "lieu_id",
        services.data ->> 'serviceObjectId'                                             AS "service_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data -> 'publics' -> 'administrative')) AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.services_all[*]') AS services (data)
    WHERE
        services.data -> 'publics' -> 'administrative' IS NOT NULL
),

familiale AS (
    SELECT
        source.data ->> 'lieu_id'                                                   AS "lieu_id",
        services.data ->> 'serviceObjectId'                                         AS "service_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data -> 'publics' -> 'familialle')) AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.services_all[*]') AS services (data)
    WHERE
        services.data -> 'publics' -> 'familialle' IS NOT NULL
),

gender AS (
    SELECT
        source.data ->> 'lieu_id'                                               AS "lieu_id",
        services.data ->> 'serviceObjectId'                                     AS "service_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data -> 'publics' -> 'gender')) AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.services_all[*]') AS services (data)
    WHERE
        services.data -> 'publics' -> 'gender' IS NOT NULL
),

other AS (
    SELECT
        source.data ->> 'lieu_id'                                              AS "lieu_id",
        services.data ->> 'serviceObjectId'                                    AS "service_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data -> 'publics' -> 'other')) AS "value"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.services_all[*]') AS services (data)
    WHERE
        services.data -> 'publics' -> 'other' IS NOT NULL
),

final AS (
    SELECT * FROM administrative
    UNION ALL
    SELECT * FROM familiale
    UNION ALL
    SELECT * FROM gender
    UNION ALL
    SELECT * FROM other
)

SELECT final.*
FROM final
INNER JOIN lieux ON final.lieu_id = lieux.lieu_id
