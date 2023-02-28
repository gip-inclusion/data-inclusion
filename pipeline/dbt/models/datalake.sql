{{
    config(
        materialized='table',
    )
}}

WITH source AS (
    {{
        dbt_utils.union_relations(
            relations=[
                source('annuaire_du_service_public', 'etablissements'),
                source('cd35', 'organisations')
                source('cd72', 'rows'),
                source('dora', 'structures'),
                source('dora', 'services'),
                source('emplois-de-linclusion', 'structures'),
                source('emplois-de-linclusion', 'services'),
                source('finess', 'etablissements'),
                source('mediation_numerique_angers', 'structures'),
                source('mediation_numerique_angers', 'services'),
                source('mediation_numerique_assembleurs', 'structures'),
                source('mediation_numerique_assembleurs', 'services'),
                source('mediation_numerique_cd49', 'structures'),
                source('mediation_numerique_cd49', 'services'),
                source('mediation_numerique_conseiller_numerique', 'structures'),
                source('mediation_numerique_conseiller_numerique', 'services'),
                source('mediation_numerique_france_services', 'structures'),
                source('mediation_numerique_france_services', 'services'),
                source('mediation_numerique_france_tiers_lieux', 'structures'),
                source('mediation_numerique_france_tiers_lieux', 'services'),
                source('mediation_numerique_francilin', 'structures'),
                source('mediation_numerique_francilin', 'services'),
                source('mediation_numerique_hinaura', 'structures'),
                source('mediation_numerique_hinaura', 'services'),
                source('mes_aides', 'aides'),
                source('mes_aides', 'garages'),
                source('siao', 'etablissements'),
                source('un_jeune_une_solution', 'benefits'),
                source('un_jeune_une_solution', 'institutions'),
            ],
        )
    }}
),

naturally_identified_data AS (
    SELECT
        CASE
            WHEN _di_source_id = 'annuaire-du-service-public' THEN data ->> 'id'
            WHEN _di_source_id = 'cd35' THEN data ->> 'ORG_ID'
            WHEN _di_source_id = 'cd72' THEN data ->> 'ID Structure'
            WHEN _di_source_id = 'dora' THEN data ->> 'id'
            WHEN _di_source_id = 'emplois-de-linclusion' THEN data ->> 'id'
            WHEN _di_source_id = 'finess' THEN data ->> 'nofinesset'
            WHEN _di_source_id ~ 'mediation-numerique' THEN data ->> 'id'
            WHEN _di_source_id = 'mes-aides' THEN data #>> '{fields,ID}'
            WHEN _di_source_id = 'siao' THEN NULL
            WHEN _di_source_id = 'un-jeune-une-solution' THEN data ->> 'id'
        END AS "natural_id",
        {{
            dbt_utils.star(from=source('dora', 'structures'))
        }}
    FROM source
    WHERE 1 IS NOT NULL
),

final AS (
    -- emulate whats currently expected by the siretisation orm
    -- TODO: update models of the siretisation orm
    SELECT
        _di_logical_date::DATE               AS "logical_date",
        _di_batch_id                         AS "batch_id",
        _di_source_id                        AS "src_alias",
        _di_source_url                       AS "src_url",
        _di_stream_s3_key                    AS "file",
        data                                 AS "data",
        GEN_RANDOM_UUID()                    AS "id",
        NOW()                                AS "created_at",
        JSONB_BUILD_OBJECT('id', natural_id) AS "data_normalized"
    FROM naturally_identified_data
    WHERE natural_id IS NOT NULL
)

SELECT * FROM final
