WITH source AS (
    SELECT *
    FROM {{ source('immersion_facilitee', 'structures') }}
),

-- temporarily allow space in identifier to remove them
-- noqa: disable=RF05
final AS (
    SELECT
        "ID"                                AS "id",
        "Name"                              AS "name",
        "Legacy Address"                    AS "legacy_address",
        "Kind"                              AS "kind",
        "Street Number And Address"         AS "street_number_and_address",
        "Post Code"                         AS "post_code",
        "City"                              AS "city",
        "Department Code"                   AS "department_code",
        "Agency Siret"                      AS "agency_siret",
        TO_DATE("Created At", 'YYYY-MM-DD') AS "created_at"
    FROM source
)
-- noqa: enable=RF05

SELECT * FROM final
