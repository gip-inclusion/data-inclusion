{% snapshot snps_mes_aides__garages %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      invalidate_hard_deletes=True,
      updated_at='_di_logical_date',
      check_cols=['id', 'data']
    )
}}

SELECT
    _di_logical_date       AS "_di_logical_date",
    data                   AS "data",
    data #>> '{fields,ID}' AS "id"
FROM {{ source('mes_aides', 'garages') }}


{% endsnapshot %}
