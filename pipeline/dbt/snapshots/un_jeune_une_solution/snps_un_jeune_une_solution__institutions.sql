{% set source_model = source('un_jeune_une_solution', 'institutions') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% snapshot snps_un_jeune_une_solution__institutions %}

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

    {% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    )

{% else %}

        WITH source AS (
            SELECT
                NULL                AS "_di_surrogate_id",
                NULL                AS "_di_source_id",
                NULL                AS "_di_stream_id",
                CAST(NULL AS DATE)  AS "_di_logical_date",
                CAST(NULL AS JSONB) AS "data"
            WHERE FALSE
        )

    {% endif %}

    SELECT
        _di_logical_date                        AS "_di_logical_date",
        data                                    AS "data",
        _di_source_id                           AS "_di_source_id",
        _di_stream_id                           AS "_di_stream_id",
        _di_source_id || '-' || (data ->> 'id') AS "_di_surrogate_id"
    FROM source

{% endsnapshot %}
