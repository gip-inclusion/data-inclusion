{{
    config(
        materialized='table',
    )
}}

WITH snapshots AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('snps_annuaire_du_service_public__etablissements'),
                ref('snps_dora__services'),
                ref('snps_dora__structures'),
                ref('snps_emplois_de_linclusion__organisations'),
                ref('snps_emplois_de_linclusion__siaes'),
                ref('snps_mes_aides__aides'),
                ref('snps_mes_aides__garages'),
                ref('snps_un_jeune_une_solution__benefits'),
                ref('snps_un_jeune_une_solution__institutions'),
            ],
            source_column_name=None,
        )
    }}
),

init_date_by_source_id AS (
    SELECT
        _di_source_id,
        MIN(dbt_valid_from) AS "init_date"
    FROM snapshots
    GROUP BY 1
),

snapshots_with_init_date AS (
    SELECT
        snapshots.*,
        init_date_by_source_id.init_date
    FROM snapshots
    INNER JOIN init_date_by_source_id
        ON snapshots._di_source_id = init_date_by_source_id._di_source_id
),

final AS (
    SELECT
        _di_logical_date                                                                AS "_di_logical_date",
        _di_source_id                                                                   AS "_di_source_id",
        snapshots.data                                                                  AS "data_next",
        snapshots_with_init_date.data                                                   AS "data_prev",
        COALESCE(snapshots._di_surrogate_id, snapshots_with_init_date._di_surrogate_id) AS "_di_surrogate_id",
        CASE
            WHEN snapshots_with_init_date.data IS NULL THEN 'AJOUT'
            WHEN snapshots.data IS NULL THEN 'SUPPRESSION'
            ELSE 'MODIFICATION'
        END                                                                             AS "type"
    FROM snapshots
    FULL JOIN snapshots_with_init_date
        ON snapshots_with_init_date._di_surrogate_id = snapshots_with_init_date._di_surrogate_id
            AND snapshots_with_init_date.dbt_valid_from = snapshots_with_init_date.dbt_valid_to
    WHERE snapshots_with_init_date.dbt_valid_from != snapshots_with_init_date.init_date
)

SELECT * FROM final
