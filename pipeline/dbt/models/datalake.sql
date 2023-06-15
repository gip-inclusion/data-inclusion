{{
    config(
        materialized='table',
        pre_hook="DROP INDEX IF EXISTS datalake_data_idx",
        post_hook="CREATE INDEX IF NOT EXISTS datalake_data_idx ON {{ this }} USING GIN (TO_TSVECTOR('french'::regconfig, COALESCE(data::TEXT, '')))"
    )
}}

WITH source AS (
    {{
        dbt_utils.union_relations(
            relations=[
                source('annuaire_du_service_public', 'etablissements'),
                source('cd35', 'organisations'),
                source('cd72', 'rows'),
                source('dora', 'structures'),
                source('dora', 'services'),
                source('emplois_de_linclusion', 'siaes'),
                source('emplois_de_linclusion', 'organisations'),
                source('finess', 'etablissements'),
                source('mediation_numerique_angers', 'structures'),
                source('mediation_numerique_angers', 'services'),
                source('mediation_numerique_assembleurs', 'structures'),
                source('mediation_numerique_assembleurs', 'services'),
                source('mediation_numerique_cd23', 'structures'),
                source('mediation_numerique_cd23', 'services'),
                source('mediation_numerique_cd33', 'structures'),
                source('mediation_numerique_cd33', 'services'),
                source('mediation_numerique_cd40', 'structures'),
                source('mediation_numerique_cd40', 'services'),
                source('mediation_numerique_cd44', 'structures'),
                source('mediation_numerique_cd44', 'services'),
                source('mediation_numerique_cd49', 'structures'),
                source('mediation_numerique_cd49', 'services'),
                source('mediation_numerique_cd87', 'structures'),
                source('mediation_numerique_cd87', 'services'),
                source('mediation_numerique_conseiller_numerique', 'structures'),
                source('mediation_numerique_conseiller_numerique', 'services'),
                source('mediation_numerique_conumm', 'structures'),
                source('mediation_numerique_conumm', 'services'),
                source('mediation_numerique_cr93', 'structures'),
                source('mediation_numerique_cr93', 'services'),
                source('mediation_numerique_fibre_64', 'structures'),
                source('mediation_numerique_fibre_64', 'services'),
                source('mediation_numerique_france_services', 'structures'),
                source('mediation_numerique_france_services', 'services'),
                source('mediation_numerique_france_tiers_lieux', 'structures'),
                source('mediation_numerique_france_tiers_lieux', 'services'),
                source('mediation_numerique_francilin', 'structures'),
                source('mediation_numerique_francilin', 'services'),
                source('mediation_numerique_hinaura', 'structures'),
                source('mediation_numerique_hinaura', 'services'),
                source('mediation_numerique_hub_antilles', 'structures'),
                source('mediation_numerique_hub_antilles', 'services'),
                source('mediation_numerique_hub_lo', 'structures'),
                source('mediation_numerique_hub_lo', 'services'),
                source('mediation_numerique_mulhouse', 'structures'),
                source('mediation_numerique_mulhouse', 'services'),
                source('mediation_numerique_res_in', 'structures'),
                source('mediation_numerique_res_in', 'services'),
                source('mediation_numerique_rhinocc', 'structures'),
                source('mediation_numerique_rhinocc', 'services'),
                source('mediation_numerique_ultra_numerique', 'structures'),
                source('mediation_numerique_ultra_numerique', 'services'),
                source('mes_aides', 'aides'),
                source('mes_aides', 'garages'),
                source('monenfant', 'creches'),
                source('siao', 'etablissements'),
                source('soliguide', 'lieux'),
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
        END AS "natural_id",  -- noqa: CV03
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
        _di_batch_id                         AS "batch_id",
        _di_source_id                        AS "src_alias",
        _di_source_url                       AS "src_url",
        _di_stream_s3_key                    AS "file",
        data                                 AS "data",
        CAST(_di_logical_date AS DATE)       AS "logical_date",
        GEN_RANDOM_UUID()                    AS "id",
        NOW()                                AS "created_at",
        JSONB_BUILD_OBJECT('id', natural_id) AS "data_normalized"
    FROM naturally_identified_data
    WHERE natural_id IS NOT NULL
),

odspep AS (
    SELECT
        NULL                               AS "batch_id",
        'odspep'                           AS "src_alias",
        NULL                               AS "src_url",
        'ressources-partenariales'         AS "file",
        TO_JSONB(t.*)                      AS "data",
        CAST('2022-01-01' AS DATE)         AS "logical_date",
        GEN_RANDOM_UUID()                  AS "id",
        NOW()                              AS "created_at",
        JSONB_BUILD_OBJECT('id', t.id_res) AS "natural_id"
    FROM {{ ref('int_odspep__enhanced_res_partenariales') }} AS t
)

SELECT * FROM final
UNION
SELECT * FROM odspep
