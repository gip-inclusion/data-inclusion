{% snapshot snps_un_jeune_une_solution__institutions %}

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
    _di_logical_date AS "_di_logical_date",
    data             AS "data",
    data ->> 'id'    AS "id"
FROM {{ source('un_jeune_une_solution', 'institutions') }}

{% endsnapshot %}
