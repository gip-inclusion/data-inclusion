WITH source AS (
    SELECT * FROM {{ source('reseau_alpha', 'structures_services') }}
),

final AS (
    SELECT
        _di_source_id                                AS "_di_source_id",

        -- Service data
        data ->> 'id'                                AS "id",
        ARRAY[data ->> 'types']                      AS "types",
        data ->> 'courriel'                          AS "courriel",
        CAST(data ->> 'date_maj' AS DATE)            AS "date_maj",
        CAST(data ->> 'cumulable' AS BOOLEAN)        AS "cumulable",
        data ->> 'telephone'                         AS "telephone",
        data ->> 'lien_source'                       AS "lien_source",
        ARRAY[data ->> 'thematiques']                AS "thematiques",
        ARRAY[data ->> 'modes_accueil']              AS "modes_accueil",
        CAST(data ->> 'contact_public' AS BOOLEAN)   AS "contact_public",
        data ->> 'contact_nom_prenom'                AS "contact_nom_prenom",
        data ->> 'zone_diffusion_nom'                AS "zone_diffusion_nom",
        data ->> 'zone_diffusion_code'               AS "zone_diffusion_code",
        data ->> 'zone_diffusion_type'               AS "zone_diffusion_type",

        -- Structure data
        data ->> 'structure_nom'                     AS "structure_nom",
        data ->> 'structure_id'                      AS "structure_id",
        CAST(data ->> 'structure_date_maj' AS DATE)  AS "structure_date_maj",
        data ->> 'structure_site_web'                AS "structure_site_web",
        data ->> 'structure_telephone'               AS "structure_telephone",
        data ->> 'structure_lien_source'             AS "structure_lien_source",
        ARRAY[data ->> 'structure_thematiques']      AS "structure_thematiques",
        ARRAY[data ->> 'structure_labels_autres']    AS "structure_labels_autres",
        data ->> 'structure_service_adresse'         AS "structure_service_adresse",
        data ->> 'structure_service_commune'         AS "structure_service_commune",
        data ->> 'structure_service_code_postal'     AS "structure_service_code_postal",
        data ->> 'structure_service_adresse_entiere' AS "structure_service_adresse_entiere"
    FROM source
)

SELECT * FROM final
