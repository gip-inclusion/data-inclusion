WITH source AS (
    SELECT * FROM {{ source('dora', 'services') }}
),

final AS (
    SELECT
        _di_source_id           AS "_di_source_id",
        ARRAY(
            SELECT * FROM
                JSONB_ARRAY_ELEMENTS_TEXT(
                    (
                        SELECT *
                        FROM JSONB_PATH_QUERY_ARRAY(data, '$.kinds[*].value')
                    )
                )
        )::TEXT []              AS "kinds",
        ARRAY(
            SELECT * FROM
                JSONB_ARRAY_ELEMENTS_TEXT(
                    (
                        SELECT *
                        FROM JSONB_PATH_QUERY_ARRAY(data, '$.categories[*].value')
                    )
                )
        )::TEXT []              AS "categories",
        ARRAY(
            SELECT * FROM
                JSONB_ARRAY_ELEMENTS_TEXT(
                    (
                        SELECT *
                        FROM JSONB_PATH_QUERY_ARRAY(data, '$.subcategories[*].value')
                    )
                )
        )::TEXT []                    AS "subcategories",
        (data ->> 'longitude')::FLOAT AS "longitude",
        (data ->> 'latitude')::FLOAT  AS "latitude",
        data ->> 'id'                 AS "id",
        data ->> 'structure'          AS "structure",
        data ->> 'name'               AS "name",
        data ->> 'fullDesc'           AS "full_desc",
        data ->> 'shortDesc'          AS "short_desc",
        data ->> 'onlineForm'         AS "online_form",
        data ->> 'feeDetails'         AS "fee_details",
        data ->> 'feeCondition'       AS "fee_condition",
        data ->> 'postalCode'         AS "postal_code",
        data ->> 'cityCode'           AS "city_code",
        data ->> 'city'               AS "city",
        data ->> 'address1'           AS "address_1",
        data ->> 'address2'           AS "address_2",
        data ->> 'recurrence'         AS "recurrence"
    FROM source
)

SELECT * FROM final
