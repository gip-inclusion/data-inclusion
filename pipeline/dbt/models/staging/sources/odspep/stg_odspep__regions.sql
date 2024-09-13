{%-
    set tables_exist = (adapter.get_relation(
        database=source('odspep', 'DD009_REGION_RESSOURCE_1').database,
        schema=source('odspep', 'DD009_REGION_RESSOURCE_1').schema,
        identifier=source('odspep', 'DD009_REGION_RESSOURCE_1').name,
    ) is not none) and (adapter.get_relation(
        database=source('odspep', 'DD009_REGION_RESSOURCE_2').database,
        schema=source('odspep', 'DD009_REGION_RESSOURCE_2').schema,
        identifier=source('odspep', 'DD009_REGION_RESSOURCE_2').name,
    ) is not none)
-%}

-- depends_on: {{ ref('stg_decoupage_administratif__regions') }}

{% if tables_exist %}

    WITH source AS (
        SELECT *
        FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_1') }}
        UNION
        SELECT *
        FROM {{ source('odspep', 'DD009_REGION_RESSOURCE_2') }}
    ),

    regions AS (
        SELECT * FROM {{ ref('stg_decoupage_administratif__regions') }}
    ),

    final AS (
        SELECT
            source."ID_REG"          AS "id",
            source."ID_REG"          AS "id_reg",
            source."ID_RES"          AS "id_res",
            source."CODE_REGION_REG" AS "code_region_reg",
            'Région'                 AS "zone_diffusion_type",
            regions.nom              AS "libelle"
        FROM source
        LEFT JOIN regions ON source."CODE_REGION_REG" = regions.code
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL AS "id",
    NULL AS "id_reg",
    NULL AS "id_res",
    NULL AS "code_region_reg",
    NULL AS "zone_diffusion_type",
    NULL AS "libelle"
WHERE FALSE

{% endif %}
