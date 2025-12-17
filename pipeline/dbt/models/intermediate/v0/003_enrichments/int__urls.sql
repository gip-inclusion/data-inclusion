{{
    config(
        materialized="incremental",
        unique_key="input_url",
        post_hook="DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ ref('int__union_urls') }} u WHERE u.url = t.input_url)"
    )
}}

WITH urls AS (
    SELECT * FROM {{ ref('int__union_urls') }}
),

next_batch AS (
    SELECT DISTINCT ON (urls.url)
        CAST('{{ run_started_at }}' AS TIMESTAMP)                             AS "last_checked_at",
        urls.url,
        {% if is_incremental() %}
            COALESCE({{ this }}.attempt_count, 0) AS attempt_count
        {% else %}
            0                                                 AS attempt_count
        {% endif %}
    FROM urls
    {% if is_incremental() %}
        LEFT JOIN {{ this }} ON urls.url = {{ this }}.input_url
        WHERE
            {{ this }}.input_url IS NULL
            OR ({{ this }}.status_code < 0 AND {{ this }}.attempt_count < 10) -- timeout
            OR {{ this }}.last_checked_at < (NOW() - INTERVAL '1 month')
    {% endif %}
    -- alphabetic order can dramatically improve performance in the best cases.
    ORDER BY urls.url
    -- we can optionnally run a very large batch resolution
    LIMIT 1000
),

final AS (
    SELECT
        next_batch.last_checked_at,
        results.input_url,
        results.url,
        results.status_code,
        CASE
            WHEN results.status_code > 0 THEN 1
            ELSE next_batch.attempt_count + 1
        END AS attempt_count,
        results.error_message
    FROM next_batch  --noqa: aliasing.unique.table
    LEFT JOIN
        processings.check_urls(
            (SELECT JSONB_AGG(next_batch.url) FROM next_batch)
        ) AS results
        ON next_batch.url = results.input_url
)

SELECT * FROM final
