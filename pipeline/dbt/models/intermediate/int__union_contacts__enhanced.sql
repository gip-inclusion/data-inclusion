WITH contacts AS (
    SELECT * FROM {{ ref('int__union_contacts') }}
),

rgpd_notices AS (
    SELECT
        courriel,
        has_hardbounced,
        was_objected_to,
        was_objected_to IS TRUE AS was_objected_to_truthy,
        has_hardbounced IS TRUE AS has_hardbounced_truthy
    FROM {{ ref('int_brevo__contacts') }}
),

final AS (
    SELECT
        contacts.source || '-' || contacts.id AS "_di_surrogate_id",
        contacts.id                           AS "id",
        contacts.source                       AS "source",
        contacts.courriel                     AS "_courriel_original",
        CASE
            WHEN
                rgpd_notices.courriel IS NULL
                OR NOT rgpd_notices.was_objected_to_truthy
                THEN contacts.contact_nom_prenom
        END                                   AS "contact_nom_prenom",
        CASE
            WHEN
                rgpd_notices.courriel IS NULL
                OR (NOT rgpd_notices.was_objected_to_truthy AND NOT rgpd_notices.has_hardbounced_truthy)
                THEN contacts.courriel
        END                                   AS "courriel",
        CASE
            WHEN
                rgpd_notices.courriel IS NULL
                OR NOT rgpd_notices.was_objected_to_truthy
                THEN contacts.telephone
        END                                   AS "telephone",
        rgpd_notices.was_objected_to          AS "rgpd_notice_was_objected_to",
        rgpd_notices.has_hardbounced          AS "rgpd_notice_has_hardbounced"
    FROM contacts
    LEFT JOIN rgpd_notices
        ON
            contacts.courriel = rgpd_notices.courriel
)

SELECT * FROM final
