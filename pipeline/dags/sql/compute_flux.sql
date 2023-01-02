/*
 * Compute the list of changes made in the logical date to the raw data in
 * the datalake.
 *
 * The process is limited to the given `batch_id` and `src_url`.
 */
BEGIN;

-- 1. Remove data from previous run (for idempotency)
DELETE FROM flux_v2
WHERE
    batch_id = '{{ run_id }}'
    AND src_url = '{{ params.src_url }}';

-- 2. Select data from the day before the logical date
WITH prev AS (
    SELECT *
    FROM
        datalake
    WHERE
        -- the data must be locally identified
        data_normalized ->> 'id' IS NOT NULL
        AND logical_date = '{{ (dag_run.logical_date - macros.timedelta(days=1)).astimezone(dag.timezone)|ds }}' -- noqa: L016
        AND src_url = '{{ params.src_url }}'
),

-- 3. Select data for the logical date
curr AS (
    SELECT *
    FROM
        datalake
    WHERE
        -- the data must be locally identified
        data_normalized ->> 'id' IS NOT NULL
        AND logical_date = '{{ dag_run.logical_date.astimezone(dag.timezone)|ds }}' -- noqa: L016
        AND src_url = '{{ params.src_url }}'
),

-- 4. Compare data
flux_for_date AS (
    SELECT
        prev.data AS "data_prev",
        curr.data AS "data_next",
        COALESCE(prev.src_alias, curr.src_alias) AS "src_alias",
        COALESCE(prev.src_url, curr.src_url) AS "src_url",
        CASE
            WHEN prev.data IS NULL THEN 'AJOUT'
            WHEN curr.data IS NULL THEN 'SUPPRESSION'
            ELSE 'MODIFICATION'
        END AS "type",
        COALESCE(
            prev.data_normalized ->> 'id',
            curr.data_normalized ->> 'id'
        ) AS "row_id"
    FROM
        prev FULL
    JOIN curr ON prev.src_url = curr.src_url
            AND prev.data_normalized ->> 'id' = curr.data_normalized ->> 'id'
    WHERE
        prev.data != curr.data
        OR prev.data IS NULL
        OR curr.data IS NULL
)

-- 5. Write results
INSERT INTO
flux_v2(
    logical_date,
    batch_id,
    src_alias,
    src_url,
    type,
    row_id,
    data_prev,
    data_next
)
SELECT
    '{{ dag_run.logical_date.astimezone(dag.timezone)|ds }}'::DATE AS "logical_date", -- noqa: L016
    '{{ run_id }}' AS "batch_id",
    src_alias,
    src_url,
    type,
    row_id,
    data_prev,
    data_next
FROM
    flux_for_date;

COMMIT;
