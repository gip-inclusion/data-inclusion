{{
    config(
        materialized="incremental",
        unique_key="url",
    )
}}

WITH urls AS (
    SELECT DISTINCT site_web AS "url"
    FROM {{ ref('int__union_structures') }}
    WHERE site_web IS NOT NULL
),

next_batch AS (
    SELECT
        urls.url,
        {% if is_incremental() %}
            COALESCE({{ this }}.attempt_count, 0) AS attempt_count
        {% else %}
        0 AS attempt_count
        {% endif %}
    FROM urls
    {% if is_incremental() %}
        LEFT JOIN {{ this }} ON urls.url = {{ this }}.input_url
        WHERE
            {{ this }}.input_url IS NULL
            OR ({{ this }}.status_code = -2 AND {{ this }}.attempt_count < 3) -- timeout
            OR {{ this }}.last_checked_at < (NOW() - INTERVAL '6 months')
    {% endif %}
    -- enables a shuffle of the URLs instead of an alphabetic order, maybe overkill
    ORDER BY MD5(urls.url)
    -- we can optionnally run a very large batch resolution
    LIMIT {{ var("check_urls_batch_size", 1000) }}
),

final AS (
    SELECT
        CAST('{{ run_started_at }}' AS TIMESTAMP) AS "last_checked_at",
        results.input_url,
        results.url,
        results.status_code,
        next_batch.attempt_count + 1                          AS attempt_count,
        results.error_message
    FROM
        next_batch
    INNER JOIN
        processings.check_urls(
            (SELECT JSONB_AGG(next_batch.url) FROM next_batch)
        ) AS results ON next_batch.url = results.input_url
)

SELECT * FROM final
