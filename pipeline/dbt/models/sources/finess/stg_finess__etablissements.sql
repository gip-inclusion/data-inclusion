WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'finess'
),

final AS (
    SELECT
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
