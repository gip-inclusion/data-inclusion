WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

di_frais_by_dora_fee_condition AS (
    SELECT x.*
    FROM (
        VALUES
        ('1', 'gratuit'),
        ('2', 'gratuit-sous-conditions'),
        ('3', 'payant'),
        ('4', 'adhesion'),
        ('5', 'pass-numerique')
    ) AS x (fee_condition, frais)
),


final AS (
    SELECT
        id                                                      AS "id",
        id                                                      AS "adresse_id",
        _di_source_id                                           AS "source",
        name                                                    AS "nom",
        short_desc                                              AS "presentation_resume",
        kinds                                                   AS "types",
        online_form                                             AS "prise_rdv",
        ARRAY(
            SELECT di_frais_by_dora_fee_condition.frais
            FROM di_frais_by_dora_fee_condition
            WHERE services.fee_condition = di_frais_by_dora_fee_condition.fee_condition
        )::TEXT []                                              AS "frais",
        fee_details                                             AS "frais_autres",
        NULL::TEXT []                                           AS "profils",
        NULL                                                    AS "pre_requis",
        NULL                                                    AS "cumulable",
        NULL                                                    AS "justificatifs",
        NULL                                                    AS "date_creation",
        NULL                                                    AS "date_suspension",
        NULL                                                    AS "lien_source",
        NULL                                                    AS "telephone",
        NULL                                                    AS "courriel",
        NULL                                                    AS "contact_public",
        NULL                                                    AS "date_maj",
        NULL                                                    AS "zone_diffusion_type",
        NULL                                                    AS "zone_diffusion_code",
        NULL                                                    AS "zone_diffusion_nom",
        NULLIF(full_desc, '')                                   AS "presentation_detail",
        NULLIF(online_form, '')                                 AS "formulaire_en_ligne",
        NULLIF(recurrence, '')                                  AS "recurrence",
        SPLIT_PART(TRIM('/' FROM structure), '/structures/', 2) AS "structure_id",
        (categories || subcategories)                           AS "thematiques",
        NULLIF(location_kinds, '{}')                            AS "modes_accueil"
    FROM services
)

SELECT * FROM final
