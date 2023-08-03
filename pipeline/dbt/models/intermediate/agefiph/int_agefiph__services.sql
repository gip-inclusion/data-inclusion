WITH services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

communes AS (
    SELECT * FROM {{ source('insee', 'communes') }}
    WHERE "TYPECOM" != 'COMD'
),

regions AS (
    SELECT * FROM {{ source('insee', 'regions') }}
),

final AS (
    SELECT
        services.id                  AS "adresse_id",
        services.contact_public      AS "contact_public",
        NULL                         AS "contact_nom_prenom",
        services.courriel            AS "courriel",
        NULL                         AS "cumulable",
        services.date_creation       AS "date_creation",
        services.date_maj            AS "date_maj",
        NULL                         AS "date_suspension",
        NULL                         AS "formulaire_en_ligne",
        NULL                         AS "frais_autres",
        services.id                  AS "id",
        NULL                         AS "justificatifs",
        NULL                         AS "lien_source",
        services.nom                 AS "nom",
        services.presentation_resume AS "presentation_resume",
        services.presentation_detail AS "presentation_detail",
        NULL                         AS "prise_rdv",
        NULL                         AS "recurrence",
        services._di_source_id       AS "source",
        services.structure_id        AS "structure_id",
        services.telephone           AS "telephone",
        services.thematiques         AS "thematiques",
        regions."REG"                AS "zone_diffusion_code",
        regions."LIBELLE"            AS "zone_diffusion_nom",
        'region'                     AS "zone_diffusion_type",
        NULL                         AS "pre_requis",
        CAST(NULL AS TEXT [])        AS "modes_accueil",
        CAST(NULL AS TEXT [])        AS "modes_orientation_accompagnateur",
        CAST(NULL AS TEXT [])        AS "modes_orientation_beneficiaire",
        CAST(NULL AS TEXT [])        AS "profils",
        CAST(NULL AS TEXT [])        AS "types",
        CAST(NULL AS TEXT [])        AS "frais"
    FROM
        services
    LEFT JOIN communes ON services.code_insee = communes."COM"
    LEFT JOIN regions ON communes."REG" = regions."REG"
)

SELECT * FROM final
