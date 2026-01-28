{% snapshot snps_doublons__nb_cross_source %}

{{
    config(
      target_schema='snapshots',
      unique_key="source_1||'--'||source_2",
      strategy='timestamp',
      updated_at='date_day',
      invalidate_hard_deletes=True,
    )
}}

    SELECT
        *,
        CAST('{{ run_started_at.strftime("%Y-%m-%d") }}' AS DATE) AS date_day
    FROM {{ ref('int__doublons_nb_cross_source_v1') }}

{% endsnapshot %}
