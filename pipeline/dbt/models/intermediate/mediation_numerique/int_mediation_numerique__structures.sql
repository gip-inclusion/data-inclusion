WITH structures AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_mediation_numerique_angers__structures'),
                ref('stg_mediation_numerique_assembleurs__structures'),
                ref('stg_mediation_numerique_cd23__structures'),
                ref('stg_mediation_numerique_cd33__structures'),
                ref('stg_mediation_numerique_cd40__structures'),
                ref('stg_mediation_numerique_cd44__structures'),
                ref('stg_mediation_numerique_cd49__structures'),
                ref('stg_mediation_numerique_cd87__structures'),
                ref('stg_mediation_numerique_conseiller_numerique__structures'),
                ref('stg_mediation_numerique_conumm__structures'),
                ref('stg_mediation_numerique_cr93__structures'),
                ref('stg_mediation_numerique_fibre_64__structures'),
                ref('stg_mediation_numerique_france_services__structures'),
                ref('stg_mediation_numerique_france_tiers_lieux__structures'),
                ref('stg_mediation_numerique_francilin__structures'),
                ref('stg_mediation_numerique_hinaura__structures'),
                ref('stg_mediation_numerique_hub_antilles__structures'),
                ref('stg_mediation_numerique_hub_lo__structures'),
                ref('stg_mediation_numerique_mulhouse__structures'),
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
        siret                 AS "siret",
        NULL                  AS "rna",
        nom                   AS "nom",
        commune               AS "commune",
        code_postal           AS "code_postal",
        NULL                  AS "code_insee",
        adresse               AS "adresse",
        NULL                  AS "complement_adresse",
        longitude             AS "longitude",
        latitude              AS "latitude",
        telephone             AS "telephone",
        courriel              AS "courriel",
        site_web              AS "site_web",
        NULL                  AS "lien_source",
        horaires_ouverture    AS "horaires_ouverture",
        NULL                  AS "accessibilite",
        labels_nationaux      AS "labels_nationaux",
        thematiques           AS "thematiques",
        NULL                  AS "typologie",
        NULL                  AS "presentation_resume",
        date_maj              AS "date_maj",
        _di_source_id         AS "source",
        NULL                  AS "presentation_detail",
        CAST(NULL AS BOOLEAN) AS "antenne",
        CAST(NULL AS TEXT []) AS "labels_autres"
    FROM structures
)

SELECT * FROM final
