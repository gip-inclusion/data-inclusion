WITH contacts AS (
    SELECT * FROM {{ ref('int__union_contacts') }}
),

brevo_contacts AS (
    SELECT * FROM {{ ref('int_brevo__contacts') }}
),

rgpd_notices AS (
    SELECT DISTINCT
        SPLIT_PART(contact_uid, ':', 1)     AS "source",
        SPLIT_PART(contact_uid, ':', 3)     AS "id",
        date_di_rgpd_opposition IS NOT NULL AS "was_objected_to",
        est_interdit                        AS "has_hardbounced"
    FROM brevo_contacts,
        UNNEST(contact_uids) AS contact_uid
),

final AS (
    SELECT
        contacts.id                  AS "id",
        contacts.source              AS "source",
        CASE
            WHEN
                rgpd_notices.id IS NULL
                OR NOT rgpd_notices.was_objected_to
                THEN contacts.contact_nom_prenom
        END                          AS "contact_nom_prenom",
        CASE
            WHEN
                rgpd_notices.id IS NULL
                OR (NOT rgpd_notices.was_objected_to AND NOT rgpd_notices.has_hardbounced)
                THEN contacts.courriel
        END                          AS "courriel",
        CASE
            WHEN
                rgpd_notices.id IS NULL
                OR NOT rgpd_notices.was_objected_to
                THEN contacts.telephone
        END                          AS "telephone",
        rgpd_notices.was_objected_to AS "rgpd_notice_was_objected_to",
        rgpd_notices.has_hardbounced AS "rgpd_notice_has_hardbounced"
    FROM contacts
    LEFT JOIN rgpd_notices
        ON
            contacts.source = rgpd_notices.source
            AND contacts.id = rgpd_notices.id
)

SELECT * FROM final
