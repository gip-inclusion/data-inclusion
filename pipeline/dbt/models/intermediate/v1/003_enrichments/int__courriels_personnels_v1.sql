WITH contacts AS (
    SELECT * FROM {{ ref('int__union_contacts_v1') }}
),

prenoms AS (
    SELECT * FROM {{ ref('stg_etat_civil__prenoms') }}
),

final AS (
    SELECT DISTINCT contacts.courriel
    FROM contacts
    INNER JOIN prenoms
        ON STARTS_WITH(
            UNACCENT(LOWER(contacts.courriel)),
            UNACCENT(LOWER(prenoms.prenom)) || '.'
        )
)

SELECT * FROM final
