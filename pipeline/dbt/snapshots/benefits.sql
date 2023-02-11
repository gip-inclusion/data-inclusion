{% snapshot benefits_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      invalidate_hard_deletes=True,
      updated_at='logical_date',
      check_cols=['id', 'slug', 'label', 'description', 'type', 'source', 'institution_id']
    )
}}

SELECT * FROM {{ ref('stg_un_jeune_une_solution__benefits') }}

{% endsnapshot %}
