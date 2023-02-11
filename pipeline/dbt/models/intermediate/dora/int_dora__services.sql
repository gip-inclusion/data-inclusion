{{
    config(
        post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
    )
}}

WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

final AS (
    SELECT
        id,
        'dora'                                        AS "source",
        name                                          AS "nom",
        short_desc                                    AS "presentation_resume",
        kinds                                         AS "types",
        online_form                                   AS "prise_rdv",
        NULL::TEXT[]                                  AS "frais",
        fee_details                                   AS "frais_autres",
        NULL::TEXT[]                                  AS "profils",
        SPLIT_PART(TRIM('/' FROM structure), '/', -1) AS "structure_id",
        (categories || subcategories)                 AS "thematiques"
    FROM services
)

SELECT * FROM final
