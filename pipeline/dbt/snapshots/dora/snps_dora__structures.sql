{% snapshot snps_dora__structures %}

{{
    config(
      target_schema='snapshots',
      unique_key='_di_surrogate_id',
      strategy='check',
      invalidate_hard_deletes=True,
      updated_at='_di_logical_date',
      check_cols=['data']
    )
}}

SELECT
    _di_logical_date                        AS "_di_logical_date",
    data                                    AS "data",
    _di_source_id || '-' || (data ->> 'id') AS "_di_surrogate_id"
FROM {{ source('dora', 'structures') }}

{% endsnapshot %}
