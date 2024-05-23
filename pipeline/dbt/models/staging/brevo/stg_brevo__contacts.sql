WITH source AS (
    {{ stg_source_header('brevo', 'contacts') }}
),

final AS (
    SELECT
        CAST((data ->> 'emailBlacklisted') AS BOOLEAN)                               AS "email_blacklisted",
        CAST((data ->> 'smsBlacklisted') AS BOOLEAN)                                 AS "sms_blacklisted",
        CAST((data ->> 'createdAt') AS TIMESTAMP WITH TIME ZONE)                     AS "created_at",
        CAST((data ->> 'modifiedAt') AS TIMESTAMP WITH TIME ZONE)                    AS "modified_at",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS(data -> 'listIds')) AS INT []) AS "list_ids",
        data ->> 'id'                                                                AS "id",
        TO_DATE(data -> 'attributes' ->> 'DATE_DI_RGPD_OPPOSITION', 'YYYY-MM-DD')    AS "date_di_rgpd_opposition",
        data -> 'attributes' ->> 'CONTACT_UIDS'                                      AS "contact_uids",
        NULLIF(TRIM(data ->> 'email'), '')                                           AS "email"
    FROM source
)

SELECT * FROM final
