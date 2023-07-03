WITH structures AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_mediation_numerique_angers__structures'),
                ref('stg_mediation_numerique_assembleurs__structures'),
                ref('stg_mediation_numerique_cd17__structures'),
                ref('stg_mediation_numerique_cd23__structures'),
                ref('stg_mediation_numerique_cd28_appui_territorial__structures'),
                ref('stg_mediation_numerique_cd33__structures'),
                ref('stg_mediation_numerique_cd40__structures'),
                ref('stg_mediation_numerique_cd44__structures'),
                ref('stg_mediation_numerique_cd49__structures'),
                ref('stg_mediation_numerique_cd87__structures'),
                ref('stg_mediation_numerique_conseiller_numerique__structures'),
                ref('stg_mediation_numerique_conumm__structures'),
                ref('stg_mediation_numerique_cr93__structures'),
                ref('stg_mediation_numerique_etapes_numerique__structures'),
                ref('stg_mediation_numerique_fibre_64__structures'),
                ref('stg_mediation_numerique_france_services__structures'),
                ref('stg_mediation_numerique_france_tiers_lieux__structures'),
                ref('stg_mediation_numerique_francilin__structures'),
                ref('stg_mediation_numerique_hinaura__structures'),
                ref('stg_mediation_numerique_hub_antilles__structures'),
                ref('stg_mediation_numerique_hub_lo__structures'),
                ref('stg_mediation_numerique_mulhouse__structures'),
                ref('stg_mediation_numerique_numi__structures'),
                ref('stg_mediation_numerique_res_in__structures'),
                ref('stg_mediation_numerique_rhinocc__structures'),
                ref('stg_mediation_numerique_ultra_numerique__structures'),
            ],
            column_override={
                "thematiques": "TEXT[]",
                "labels_nationaux": "TEXT[]",
            },
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        id                    AS "id",
        id                    AS "adresse_id",
        siret                 AS "siret",
        NULL                  AS "rna",
        nom                   AS "nom",
        telephone             AS "telephone",
        courriel              AS "courriel",
        site_web              AS "site_web",
        NULL                  AS "lien_source",
        horaires_ouverture    AS "horaires_ouverture",
        NULL                  AS "accessibilite",
        labels_nationaux      AS "labels_nationaux",
        thematiques           AS "thematiques",
        typologie             AS "typologie",
        presentation_resume   AS "presentation_resume",
        presentation_detail   AS "presentation_detail",
        date_maj              AS "date_maj",
        _di_source_id         AS "source",
        NULL                  AS "presentation_detail",
        labels_autres         AS "labels_autres",
        CAST(NULL AS BOOLEAN) AS "antenne"
    FROM structures
)

SELECT * FROM final
