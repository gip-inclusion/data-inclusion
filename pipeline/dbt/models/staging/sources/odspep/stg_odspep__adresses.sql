{% set source_model = source('odspep', 'DD009_ADRESSE') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    final AS (
        SELECT
            "ID_ADR"                            AS "id",
            "ID_ADR"                            AS "id_adr",
            "L1_IDENTIFICATION_DEST_ADR"        AS "l1_identification_dest_adr",
            "L2_IDENTITE_DEST_ADR"              AS "l2_identite_dest_adr",
            "L4_NUMERO_LIB_VOIE_ADR"            AS "l4_numero_lib_voie_adr",
            "L3_COMPLEMENT_ADR"                 AS "l3_complement_adr",
            "L5_MENTION_ADR"                    AS "l5_mention_adr",
            "L7_PAYS_ADR"                       AS "l7_pays_adr",
            "LATITUDE_ADR"::FLOAT               AS "latitude_adr",
            "LONGITUDE_ADR"::FLOAT              AS "longitude_adr",
            "EST_NORMALISEE_ADR"::INT           AS "est_normalisee_adr",
            "CODE_POSTAL_ADR"                   AS "code_postal_adr",
            "LIBELLE_COMMUNE_ADR"               AS "libelle_commune_adr",
            NULLIF("CODE_COMMUNE_ADR", 'XXXXX') AS "code_commune_adr"
        FROM source
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL        AS "id",
    NULL        AS "id_adr",
    NULL        AS "l1_identification_dest_adr",
    NULL        AS "l2_identite_dest_adr",
    NULL        AS "l4_numero_lib_voie_adr",
    NULL        AS "l3_complement_adr",
    NULL        AS "l5_mention_adr",
    NULL        AS "l7_pays_adr",
    NULL::FLOAT AS "latitude_adr",
    NULL::FLOAT AS "longitude_adr",
    NULL::INT   AS "est_normalisee_adr",
    NULL        AS "code_postal_adr",
    NULL        AS "libelle_commune_adr",
    NULL        AS "code_commune_adr"
WHERE FALSE

{% endif %}
