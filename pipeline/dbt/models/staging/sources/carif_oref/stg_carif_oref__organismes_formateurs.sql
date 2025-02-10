WITH source AS (
    {{ stg_source_header('carif_oref', 'formations') }}
),

final AS (
    SELECT
        DISTINCT ON (numero)
        NULLIF(TRIM(organismes_formateurs.data ->> '@numero'), '')                     AS "numero",
        NULLIF(TRIM(organismes_formateurs.data ->> 'raison-sociale-formateur'), '')    AS "raison_sociale_formateur",
        NULLIF(TRIM(organismes_formateurs.data -> '@SIRET-formateur' ->> 'SIRET'), '') AS "siret_formateur__siret"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.action[*].organisme\-formateur[*]') AS organismes_formateurs (data)
    WHERE organismes_formateurs.data IS NOT NULL
)

SELECT * FROM final
