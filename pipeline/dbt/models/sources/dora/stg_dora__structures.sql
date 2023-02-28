WITH source AS (
    SELECT * FROM {{ source('dora', 'structures') }}
),

final AS (
    SELECT
        (data ->> 'longitude')::FLOAT                      AS "longitude",
        (data ->> 'latitude')::FLOAT                       AS "latitude",
        TO_DATE(data ->> 'modificationDate', 'YYYY-MM-DD') AS "modification_date",
        data ->> 'id'                                      AS "id",
        data ->> 'siret'                                   AS "siret",
        data ->> 'name'                                    AS "name",
        data ->> 'city'                                    AS "city",
        data ->> 'postalCode'                              AS "postal_code",
        data ->> 'cityCode'                                AS "city_code",
        data ->> 'address1'                                AS "address_1",
        data ->> 'address2'                                AS "address_2",
        data #>> '{typology,value}'                        AS "typology",
        data ->> 'phone'                                   AS "phone",
        data ->> 'email'                                   AS "email",
        data ->> 'url'                                     AS "url",
        data ->> 'shortDesc'                               AS "short_desc",
        data ->> 'fullDesc'                                AS "full_desc",
        data ->> 'linkOnSource'                            AS "link_on_source"
    FROM source
)

SELECT * FROM final
