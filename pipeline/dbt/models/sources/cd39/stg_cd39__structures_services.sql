WITH source AS (
    SELECT * FROM {{ source('cd39', 'structures_services') }}
),


final AS (
    SELECT
        _di_source_id                                    AS "_di_source_id",
        data ->> 'structure_id'                           AS "structure_id",
        data ->> 'id_service'                           AS "id_service",
        data ->> 'nom_structure'                           AS "nom_structure",
        data ->> 'typologie'                           AS "typologie",
        data ->> 'courriel_structure'                           AS "courriel_structure",
        data ->> 'code_postal'                           AS "code_postal",
        data ->> 'commune'                           AS "commune",
        data ->> 'adresse'                           AS "adresse",
        data ->> 'profils'                           AS "profils",
        data ->> 'nom_service'                           AS "nom_service",
        data ->> 'presentation_detail'                           AS "presentation_detail",
        data ->> 'thematiques'                           AS "thematiques",
        data ->> 'site_web'                           AS "site_web",
        data ->> 'courriel_service'                           AS "courriel_service",
        data ->> 'telephone'                           AS "telephone"
    FROM source
)

SELECT * FROM final
