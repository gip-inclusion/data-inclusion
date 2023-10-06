{% set source_model = source('finess', 'etablissements') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

WITH source AS (
    SELECT
        NULL                AS "_di_source_id",
        CAST(NULL AS JSONB) AS "data"
    WHERE FALSE
),

{% endif %}

final AS (
    SELECT
        _di_source_id                         AS "_di_source_id",
        (data ->> 'coordxet')::FLOAT          AS "coordxet",
        (data ->> 'coordyet')::FLOAT          AS "coordyet",
        data ->> 'nofinesset'                 AS "id",
        data ->> 'categagretab'               AS "categagretab",
        data ->> 'categetab'                  AS "categetab",
        data ->> 'nofinesset'                 AS "nofinesset",
        data ->> 'siret'                      AS "siret",
        data ->> 'rs'                         AS "rs",
        data ->> 'ligneacheminement'          AS "ligneacheminement",
        data ->> 'departement'                AS "departement",
        data ->> 'commune'                    AS "commune",
        data ->> 'compldistrib'               AS "compldistrib",
        data ->> 'numvoie'                    AS "numvoie",
        data ->> 'compvoie'                   AS "compvoie",
        data ->> 'typvoie'                    AS "typvoie",
        data ->> 'voie'                       AS "voie",
        data ->> 'lieuditbp'                  AS "lieuditbp",
        data ->> 'libdepartement'             AS "libdepartement",
        data ->> 'telephone'                  AS "telephone",
        data ->> 'libcategetab'               AS "libcategetab",
        data ->> 'libcategagretab'            AS "libcategagretab",
        TO_DATE(data ->> 'maj', 'YYYY-MM-DD') AS "maj"
    FROM source
)

SELECT * FROM final
