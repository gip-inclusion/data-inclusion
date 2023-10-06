{% set source_model = source('immersion_facilitee', 'structures') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
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

{% else %}

SELECT
    NULL               AS "id",
    NULL               AS "name",
    NULL               AS "legacy_address",
    NULL               AS "kind",
    NULL               AS "street_number_and_address",
    NULL               AS "post_code",
    NULL               AS "city",
    NULL               AS "department_code",
    NULL               AS "agency_siret",
    CAST(NULL AS DATE) AS "created_at"
WHERE FALSE

{% endif %}
