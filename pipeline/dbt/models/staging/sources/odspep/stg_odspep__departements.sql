{% set source_model = source('odspep', 'DD009_DEPARTEMENT_RESSOURCE') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

-- depends_on: {{ ref('stg_code_officiel_geographique__departements') }}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    departements AS (
        SELECT * FROM {{ ref('stg_code_officiel_geographique__departements') }}
    ),

    final AS (
        SELECT
            source."ID_DPT"               AS "id",
            source."ID_DPT"               AS "id_dpt",
            source."ID_RES"               AS "id_res",
            source."CODE_DEPARTEMENT_DPT" AS "code_departement_dpt",
            departements.libelle          AS "libelle"
        FROM source
        LEFT JOIN departements ON source."CODE_DEPARTEMENT_DPT" = departements.code
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL AS "id",
    NULL AS "id_dpt",
    NULL AS "id_res",
    NULL AS "code_departement_dpt",
    NULL AS "libelle"
WHERE FALSE

{% endif %}
