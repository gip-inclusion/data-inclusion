{% set source_model = source('odspep', 'DD009_COMMUNE_RESSOURCE') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

-- depends_on: {{ ref('stg_decoupage_administratif__communes') }}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    communes AS (
        SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
    ),

    final AS (
        SELECT
            source."ID_COM" AS "id",
            source."ID_COM",
            source."ID_RES",
            source."CODE_COMMUNE_COM",
            communes.nom    AS "libelle"
        FROM source
        LEFT JOIN communes ON source."CODE_COMMUNE_COM" = communes.code
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL AS "id",
    NULL AS "id_com",
    NULL AS "id_res",
    NULL AS "code_commune_com",
    NULL AS "libelle"
WHERE FALSE

{% endif %}
