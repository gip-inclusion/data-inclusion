{% set presentation %}
structures.nom || ' propose des services : ' || ARRAY_TO_STRING(
    ARRAY(
        SELECT LOWER(di_thematiques.label)
        FROM UNNEST(services.thematiques) AS t (value)
        INNER JOIN di_thematiques ON t.value = di_thematiques.value
    ),
', ') || '.'
{% endset %}

WITH services AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_mediation_numerique_angers__services'),
                ref('stg_mediation_numerique_assembleurs__services'),
                ref('stg_mediation_numerique_cd17__services'),
                ref('stg_mediation_numerique_cd23__services'),
                ref('stg_mediation_numerique_cd28_appui_territorial__services'),
                ref('stg_mediation_numerique_cd33__services'),
                ref('stg_mediation_numerique_cd40__services'),
                ref('stg_mediation_numerique_cd44__services'),
                ref('stg_mediation_numerique_cd49__services'),
                ref('stg_mediation_numerique_cd87__services'),
                ref('stg_mediation_numerique_conseiller_numerique__services'),
                ref('stg_mediation_numerique_conumm__services'),
                ref('stg_mediation_numerique_cr93__services'),
                ref('stg_mediation_numerique_etapes_numerique__services'),
                ref('stg_mediation_numerique_fibre_64__services'),
                ref('stg_mediation_numerique_france_services__services'),
                ref('stg_mediation_numerique_france_tiers_lieux__services'),
                ref('stg_mediation_numerique_francilin__services'),
                ref('stg_mediation_numerique_hinaura__services'),
                ref('stg_mediation_numerique_hub_antilles__services'),
                ref('stg_mediation_numerique_hub_lo__services'),
                ref('stg_mediation_numerique_mulhouse__services'),
                ref('stg_mediation_numerique_numi__services'),
                ref('stg_mediation_numerique_res_in__services'),
                ref('stg_mediation_numerique_rhinocc__services'),
                ref('stg_mediation_numerique_ultra_numerique__services'),
            ],
            column_override={
                "types": "TEXT[]",
                "profils": "TEXT[]",
                "thematiques": "TEXT[]",
                "frais": "TEXT[]",
            },
            source_column_name=None,
        )
    }}
),

structures AS (
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

di_thematiques AS (
    SELECT * FROM {{ ref('thematiques') }}
),

final AS (
    SELECT
        services.id                                                                                    AS "id",
        services.nom                                                                                   AS "nom",
        services.prise_rdv                                                                             AS "prise_rdv",
        services.frais                                                                                 AS "frais",
        services.profils                                                                               AS "profils",
        services.structure_id                                                                          AS "structure_id",
        services.thematiques                                                                           AS "thematiques",
        services._di_source_id                                                                         AS "source",
        NULL                                                                                           AS "pre_requis",
        NULL                                                                                           AS "cumulable",
        NULL                                                                                           AS "justificatifs",
        NULL                                                                                           AS "formulaire_en_ligne",
        structures.commune                                                                             AS "commune",
        structures.code_postal                                                                         AS "code_postal",
        NULL                                                                                           AS "code_insee",
        structures.adresse                                                                             AS "adresse",
        NULL                                                                                           AS "complement_adresse",
        structures.longitude                                                                           AS "longitude",
        structures.latitude                                                                            AS "latitude",
        NULL                                                                                           AS "recurrence",
        NULL                                                                                           AS "date_creation",
        NULL                                                                                           AS "date_suspension",
        NULL                                                                                           AS "lien_source",
        structures.telephone                                                                           AS "telephone",
        structures.courriel                                                                            AS "courriel",
        TRUE                                                                                           AS "contact_public",
        structures.date_maj                                                                            AS "date_maj",
        NULL                                                                                           AS "zone_diffusion_type",
        NULL                                                                                           AS "zone_diffusion_code",
        NULL                                                                                           AS "zone_diffusion_nom",
        CAST(NULL AS TEXT)                                                                             AS "frais_autres",
        CASE WHEN CARDINALITY(services.types) > 0 THEN services.types ELSE ARRAY['accompagnement'] END AS "types",
        ARRAY['en-presentiel']                                                                         AS "modes_accueil",
        {{ truncate_text(presentation) }}                                                              AS "presentation_resume",
        {{ presentation }}                                                                             AS "presentation_detail"
    FROM services
    LEFT JOIN structures ON services.structure_id = structures.id AND services._di_source_id = structures._di_source_id
)

SELECT * FROM final
