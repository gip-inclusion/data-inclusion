WITH services AS (
    SELECT * FROM {{ ref('stg_soliguide__services') }}
),

publics AS (
    SELECT
        services.lieu_id,
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data #> '{publics,familialle}')) AS "value"
    FROM services
    UNION ALL
    SELECT
        services.lieu_id,
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data #> '{publics,gender}')) AS "value"
    FROM services
    UNION ALL
    SELECT
        services.lieu_id,
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data #> '{publics,administrative}')) AS "value"
    FROM services
    UNION ALL
    SELECT
        services.lieu_id,
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(services.data #> '{publics,other}')) AS "value"
    FROM services
),

grouped AS (
    SELECT
        lieu_id,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT value), NULL) AS publics
    FROM publics
    GROUP BY lieu_id
),

with_tous_publics AS (
    SELECT
        grouped.lieu_id,
        CASE
            WHEN grouped.publics = '{}' THEN NULL
            WHEN CARDINALITY(grouped.publics) = 19 THEN ARRAY['tous-publics']
            ELSE
                grouped.publics
        END AS publics
    FROM
        grouped
),

mapped AS (
    SELECT
        with_tous_publics.lieu_id,
        ARRAY(
            SELECT DISTINCT public_mapping.di_public
            FROM UNNEST(with_tous_publics.publics) AS input_public
            CROSS JOIN
                LATERAL (
                    VALUES
                    ('addiction', 'personnes-en-situation-durgence'),
                    ('asylum', 'personnes-exilees'),
                    ('couple', 'familles'),
                    ('family', 'familles'),
                    ('handicap', 'personnes-en-situation-de-handicap'),
                    ('hiv', 'personnes-en-situation-de-handicap'),
                    ('isolated', 'personnes-en-situation-durgence'),
                    ('isolated', 'seniors'),
                    ('lgbt', NULL),
                    ('men', NULL),
                    ('mentalHealth', 'personnes-en-situation-de-handicap'),
                    ('pregnant', 'femmes'),
                    ('pregnant', 'familles'),
                    ('prison', 'personnes-en-situation-juridique-specifique'),
                    ('prostitution', NULL),
                    ('refugee', 'personnes-exilees'),
                    ('regular', NULL),
                    ('student', 'etudiants'),
                    ('undocumented', 'personnes-exilees'),
                    ('violence', 'personnes-en-situation-durgence'),
                    ('women', 'femmes'),
                    ('tous-publics', 'tous-publics')
                ) AS public_mapping (soliguide_public, di_public)
            WHERE
                public_mapping.soliguide_public = input_public
                AND public_mapping.di_public IS NOT NULL
        ) AS publics
    FROM
        with_tous_publics
),

final AS (
    SELECT *
    FROM
        mapped
)

SELECT * FROM final
