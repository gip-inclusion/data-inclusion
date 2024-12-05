{% snapshot snps_doublons__nb_mono_source %}

{{
    config(
      target_schema='snapshots',
      unique_key="source",
      strategy='timestamp',
      updated_at='date_day',
      invalidate_hard_deletes=True,
    )
}}

    SELECT
        *,
        CAST('{{ run_started_at.strftime("%Y-%m-%d") }}' AS DATE) AS date_day
    FROM {{ ref('int__doublons_nb_mono_source') }}

{% endsnapshot %}
