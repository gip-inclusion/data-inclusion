{% snapshot snps_soliguide__lieux %}

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
    _di_logical_date                                 AS "_di_logical_date",
    data                                             AS "data",
    _di_source_id || '-' || (data #>> '{fields,ID}') AS "_di_surrogate_id",
    _di_source_id                                    AS "_di_source_id",
    _di_stream_id                                    AS "_di_stream_id"
FROM {{ source('soliguide', 'lieux') }}


{% endsnapshot %}
