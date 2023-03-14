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

snapshots_with_first_extraction_date AS (
    SELECT
        snapshots.*,
        init_date_by_source_id.first_extraction_date
    FROM snapshots
    INNER JOIN init_date_by_source_id
        ON snapshots._di_source_id = init_date_by_source_id._di_source_id
),

inserts AS (
    SELECT
        snapshots_with_first_extraction_date.dbt_valid_from   AS "_di_logical_date",
        snapshots_with_first_extraction_date._di_source_id    AS "_di_source_id",
        snapshots_with_first_extraction_date._di_stream_id    AS "_di_stream_id",
        snapshots_with_first_extraction_date.data             AS "data_next",
        NULL::JSONB                                           AS "data_prev",
        snapshots_with_first_extraction_date._di_surrogate_id AS "_di_surrogate_id",
        'AJOUT'                                               AS "type"
    FROM snapshots_with_first_extraction_date
    LEFT JOIN snapshots_with_first_extraction_date AS prev
        ON snapshots_with_first_extraction_date._di_surrogate_id = prev._di_surrogate_id
            AND snapshots_with_first_extraction_date.dbt_valid_from = prev.dbt_valid_to
    WHERE prev.data IS NULL
        -- ignore additions from the first date of extraction
        AND snapshots_with_first_extraction_date._di_logical_date > snapshots_with_first_extraction_date.first_extraction_date  -- noqa: L016yy
),

updates AS (
    SELECT
        snapshots_with_first_extraction_date.dbt_valid_to     AS "_di_logical_date",
        snapshots_with_first_extraction_date._di_source_id    AS "_di_source_id",
        snapshots_with_first_extraction_date._di_stream_id    AS "_di_stream_id",
        next_.data                                            AS "data_next",
        snapshots_with_first_extraction_date.data             AS "data_prev",
        snapshots_with_first_extraction_date._di_surrogate_id AS "_di_surrogate_id",
        'MODIFICATION'                                        AS "type"
    FROM snapshots_with_first_extraction_date
    INNER JOIN snapshots_with_first_extraction_date AS next_
        ON snapshots_with_first_extraction_date._di_surrogate_id = next_._di_surrogate_id
            AND snapshots_with_first_extraction_date.dbt_valid_to = next_.dbt_valid_from
),

deletes AS (
    SELECT
        snapshots_with_first_extraction_date.dbt_valid_to     AS "_di_logical_date",
        snapshots_with_first_extraction_date._di_source_id    AS "_di_source_id",
        snapshots_with_first_extraction_date._di_stream_id    AS "_di_stream_id",
        NULL::JSONB                                           AS "data_next",
        snapshots_with_first_extraction_date.data             AS "data_prev",
        snapshots_with_first_extraction_date._di_surrogate_id AS "_di_surrogate_id",
        'SUPPRESSION'                                         AS "type"
    FROM snapshots_with_first_extraction_date
    LEFT JOIN snapshots_with_first_extraction_date AS next_
        ON snapshots_with_first_extraction_date._di_surrogate_id = next_._di_surrogate_id
            AND snapshots_with_first_extraction_date.dbt_valid_to = next_.dbt_valid_from
    WHERE snapshots_with_first_extraction_date.dbt_valid_to IS NOT NULL
        AND next_.data IS NULL
),

final AS (
    SELECT * FROM inserts
    UNION ALL
    SELECT * FROM updates
    UNION ALL
    SELECT * FROM deletes
)

SELECT * FROM final
