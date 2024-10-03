WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

di_profil_by_dora_profil AS (
    -- dora's thematiques are not yet normalized
    SELECT x.*
    FROM (
        VALUES
        ('Adultes', 'adultes'),
        ('Femmes', 'femmes'),
        ('Public bénéficiaire du Revenu de Solidarité Active (RSA)', 'beneficiaires-rsa'),
        ('Demandeur d''emploi', 'demandeurs-demploi')
    ) AS x (dora_profil, di_profil)
),

final AS (
    SELECT
        services.id                                      AS "adresse_id",
        services.contact_public                          AS "contact_public",
        services.cumulable                               AS "cumulable",
        services.date_creation::DATE                     AS "date_creation",
        services.date_maj::DATE                          AS "date_maj",
        services.date_suspension::DATE                   AS "date_suspension",
        services.formulaire_en_ligne                     AS "formulaire_en_ligne",
        services.frais_autres                            AS "frais_autres",
        services.id                                      AS "id",
        services.justificatifs                           AS "justificatifs",
        services.lien_source                             AS "lien_source",
        services.modes_accueil                           AS "modes_accueil",
        services.modes_orientation_accompagnateur        AS "modes_orientation_accompagnateur",
        services.modes_orientation_accompagnateur_autres AS "modes_orientation_accompagnateur_autres",
        /* The 'professionnel' condition is not included in the associated seed. Therefore, after discussing it,
        we decided to convert it into a null value to still take it in account in our tables and avoid a global schema change */
        CASE
            WHEN 'professionnel' = ANY(services.modes_orientation_beneficiaire) THEN NULL
            ELSE services.modes_orientation_beneficiaire
        END                                              AS "modes_orientation_beneficiaire",
        services.modes_orientation_beneficiaire_autres   AS "modes_orientation_beneficiaire_autres",
        services.nom                                     AS "nom",
        services.presentation_resume                     AS "presentation_resume",
        services.presentation_detail                     AS "presentation_detail",
        services.prise_rdv                               AS "prise_rdv",
        ARRAY(
            SELECT di_profil_by_dora_profil.di_profil
            FROM di_profil_by_dora_profil
            WHERE di_profil_by_dora_profil.dora_profil = ANY(services.profils)
        )::TEXT []                                       AS "profils",
        services.recurrence                              AS "recurrence",
        services._di_source_id                           AS "source",
        services.structure_id                            AS "structure_id",
        /* Dora made some changes to the thematics which add '--autre' in some cases.
        We do not need them, thus they are removed from the table in order to pass the checks */
        ARRAY(
            SELECT REPLACE(unnested, '--autre', '')
            FROM UNNEST(services.thematiques) AS unnested
        )                                                AS "thematiques",
        services.types                                   AS "types",
        services.zone_diffusion_code                     AS "zone_diffusion_code",
        services.zone_diffusion_nom                      AS "zone_diffusion_nom",
        services.zone_diffusion_type                     AS "zone_diffusion_type",
        services.pre_requis                              AS "pre_requis",
        NULL                                             AS "page_web",
        services.contact_nom_prenom                      AS "contact_nom_prenom",
        services.courriel                                AS "courriel",
        services.telephone                               AS "telephone",
        CASE
            WHEN services.frais IS NULL THEN NULL
            ELSE ARRAY[services.frais]
        END                                              AS "frais"
    FROM services
)

SELECT * FROM final
