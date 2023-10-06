{% set source_model = source('odspep', 'DD009_CONTACT') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    final AS (
        SELECT
            "ID_CTC"                  AS "id",
            "ID_CTC"                  AS "id_ctc",
            "TEL_1_CTC"               AS "tel_1_ctc",
            "TEL_2_CTC"               AS "tel_2_ctc",
            "FAX_CTC"                 AS "fax_ctc",
            TRIM("SITE_INTERNET_CTC") AS "site_internet_ctc",
            TRIM("MAIL_CTC")          AS "mail_ctc"

        FROM source
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL AS "id",
    NULL AS "id_ctc",
    NULL AS "tel_1_ctc",
    NULL AS "tel_2_ctc",
    NULL AS "fax_ctc",
    NULL AS "site_internet_ctc",
    NULL AS "mail_ctc"
WHERE FALSE

{% endif %}
