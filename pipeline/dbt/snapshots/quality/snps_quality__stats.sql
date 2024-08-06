{% snapshot snps_quality__stats %}

{{
    config(
      target_schema='snapshots',
      unique_key="source||'-'||stream",
      strategy='timestamp',
      updated_at='date_day',
      invalidate_hard_deletes=True,
    )
}}

    SELECT
        source || '-' || stream AS id,
        *
    FROM {{ ref('int_quality__stats') }}

{% endsnapshot %}
