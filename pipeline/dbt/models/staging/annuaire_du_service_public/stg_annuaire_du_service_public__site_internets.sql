WITH source AS (
    SELECT * FROM {{ source('annuaire_du_service_public', 'etablissements') }}
),


final AS (
    SELECT
        source.data ->> 'id'              AS "etablissement_id",
        site_internets.data ->> 'valeur'  AS "valeur",
        site_internets.data ->> 'libelle' AS "libelle"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.site_internet[*]')) AS site_internets (data)
)

SELECT * FROM final
