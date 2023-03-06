WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_CONTACT') }}
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
