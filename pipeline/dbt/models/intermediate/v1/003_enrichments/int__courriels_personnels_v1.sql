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
        ON STARTS_WITH(contacts.courriel, (prenoms.prenom || '.'))
    WHERE
        -- focus on mainstream email providers
        SPLIT_PART(SPLIT_PART(contacts.courriel, '@', 2), '.', -2) -- 2nd level domain
        IN (
            -- list compiled by looking at most common domains in the dataset
            'free',
            'gmail',
            'hotmail',
            'laposte',
            'nordnet',
            'orange',
            'outlook',
            'sfr',
            'wanadoo',
            'yahoo'
        )
        -- ignore common patterns for non personal emails
        AND NOT SPLIT_PART(contacts.courriel, '@', 1) ~ 'mairie|commune|adil|services|ccas'
)

SELECT * FROM final
