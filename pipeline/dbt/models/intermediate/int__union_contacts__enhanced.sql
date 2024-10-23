WITH contacts AS (
    SELECT * FROM {{ ref('int__union_contacts') }}
),

rgpd_notices AS (
    SELECT
        courriel,
        has_hardbounced,
        was_objected_to
    FROM {{ ref('int_brevo__contacts') }}
),

final AS (
    SELECT
        contacts._di_surrogate_id    AS "_di_surrogate_id",
        contacts.id                  AS "id",
        contacts.source              AS "source",
        CASE
            WHEN
                rgpd_notices.courriel IS NULL
                OR NOT rgpd_notices.was_objected_to
                THEN contacts.contact_nom_prenom
        END                          AS "contact_nom_prenom",
        CASE
            WHEN
                rgpd_notices.courriel IS NULL
                OR (NOT rgpd_notices.was_objected_to AND NOT rgpd_notices.has_hardbounced)
                THEN contacts.courriel
        END                          AS "courriel",
        CASE
            WHEN
                rgpd_notices.courriel IS NULL
                OR NOT rgpd_notices.was_objected_to
                THEN contacts.telephone
        END                          AS "telephone",
        rgpd_notices.was_objected_to AS "rgpd_notice_was_objected_to",
        rgpd_notices.has_hardbounced AS "rgpd_notice_has_hardbounced"
    FROM contacts
    LEFT JOIN rgpd_notices
        ON
            contacts.courriel = rgpd_notices.courriel
)

SELECT * FROM final
