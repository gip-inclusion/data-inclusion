{% snapshot snps_doublons__nb_multi_source %}

{{
    config(
      target_schema='snapshots',
      unique_key="source",
      strategy='timestamp',
      updated_at='date_day',
      invalidate_hard_deletes=True,
    )
}}

    SELECT *
    FROM {{ ref('int__doublons_nb_multi_source') }}

{% endsnapshot %}
