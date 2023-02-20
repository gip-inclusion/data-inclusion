WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

final AS (
    SELECT
        id_res              AS "id",
        "service"           AS "nom",
        'odspep'            AS "source",
        service_description AS "presentation_resume",
        date_fin_valid      AS "date_suspension",
        date_derniere_modif AS "date_modification",
        CASE WHEN prescriptible
            THEN ARRAY['demandeur-demploi']::TEXT[]
        END                 AS "profils",
        -- Mapping: https://www.notion.so/dora-beta/Explorer-ODSPEP-du-23-01-22-48e1d505c7cb4fdb9e3ec789449e7ca7#24743be5aac94306b6261e8e6c2f748c
        CASE type_res_part
            WHEN '3' THEN ARRAY['creation-dactivite']::TEXT[]
            WHEN '5' THEN ARRAY['handicap']::TEXT[]
            WHEN '6' THEN ARRAY['accompagnement-social-et-professionnel-personnalise']::TEXT[]
            WHEN '8' THEN ARRAY['mobilite']::TEXT[]
            WHEN '9' THEN ARRAY['numerique']::TEXT[]
        END                 AS "thematiques"
    -- TODO: map the columns perimetre_geo with zone_de_diffusion (https://github.com/betagouv/data-inclusion/issues/30)

    FROM ressources_partenariales
    ORDER BY 1

)

SELECT * FROM final
