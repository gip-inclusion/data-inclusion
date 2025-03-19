WITH contacts AS (
    SELECT * FROM {{ ref('int__union_contacts') }}
),

courriels_verifies AS (
    SELECT
        courriel,
        has_hardbounced,
        was_objected_to
    FROM {{ ref('int__courriels_verifies') }}
),

final AS (
    SELECT
        contacts._di_surrogate_id          AS "_di_surrogate_id",
        contacts.id                        AS "id",
        contacts.source                    AS "source",
        CASE
            WHEN
                courriels_verifies.courriel IS NULL
                OR NOT courriels_verifies.was_objected_to
                THEN contacts.contact_nom_prenom
        END                                AS "contact_nom_prenom",
        CASE
            WHEN
                courriels_verifies.courriel IS NULL
                OR (NOT courriels_verifies.was_objected_to AND NOT courriels_verifies.has_hardbounced)
                THEN contacts.courriel
        END                                AS "courriel",
        CASE
            WHEN
                courriels_verifies.courriel IS NULL
                OR NOT courriels_verifies.was_objected_to
                THEN contacts.telephone
        END                                AS "telephone",
        courriels_verifies.was_objected_to AS "rgpd_notice_was_objected_to",
        courriels_verifies.has_hardbounced AS "rgpd_notice_has_hardbounced"
    FROM contacts
    LEFT JOIN courriels_verifies
        ON
            contacts.courriel = courriels_verifies.courriel
)

SELECT * FROM final
