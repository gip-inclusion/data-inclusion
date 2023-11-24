WITH source AS (
    {{ stg_source_header('brevo', 'contacts') }}
),

final AS (
    SELECT
        (data ->> 'emailBlacklisted')::BOOLEAN                                    AS "email_blacklisted",
        (data ->> 'smsBlacklisted')::BOOLEAN                                      AS "sms_blacklisted",
        (data ->> 'createdAt')::TIMESTAMP WITH TIME ZONE                          AS "created_at",
        (data ->> 'modifiedAt')::TIMESTAMP WITH TIME ZONE                         AS "modified_at",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS(data -> 'listIds'))::INT []      AS "list_ids",
        data ->> 'id'                                                             AS "id",
        TO_DATE(data -> 'attributes' ->> 'DATE_DI_RGPD_OPPOSITION', 'YYYY-MM-DD') AS "date_di_rgpd_opposition",
        data -> 'attributes' ->> 'CONTACT_UIDS'                                   AS "contact_uids",
        NULLIF(TRIM(data ->> 'email'), '')                                        AS "email"
    FROM source
)

SELECT * FROM final
