WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

final AS (
    SELECT
        id,
        _di_source_id                                           AS "source",
        name                                                    AS "nom",
        short_desc                                              AS "presentation_resume",
        kinds                                                   AS "types",
        online_form                                             AS "prise_rdv",
        NULL::TEXT[]                                            AS "frais",
        fee_details                                             AS "frais_autres",
        NULL::TEXT[]                                            AS "profils",
        SPLIT_PART(TRIM('/' FROM structure), '/structures/', 2) AS "structure_id",
        (categories || subcategories)                           AS "thematiques"
    FROM services
)

SELECT * FROM final
