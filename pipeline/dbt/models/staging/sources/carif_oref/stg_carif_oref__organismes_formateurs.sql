WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT DISTINCT ON (1)
        NULLIF(TRIM(organismes_formateurs.data ->> '@numero'), '')                    AS "numero",
        NULLIF(TRIM(organismes_formateurs.data ->> 'raison-sociale-formateur'), '')   AS "raison_sociale_formateur",
        NULLIF(TRIM(organismes_formateurs.data -> 'SIRET-formateur' ->> 'SIRET'), '') AS "siret_formateur__siret"
    FROM
        source,  -- noqa: structure.unused_join
        JSONB_PATH_QUERY(source.data, '$.action[*].organisme\-formateur[*]') AS organismes_formateurs (data)  -- noqa: structure.unused_join
    WHERE organismes_formateurs.data IS NOT NULL
)

SELECT * FROM final
