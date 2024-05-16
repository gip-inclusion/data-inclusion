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

blocked_contacts AS (
    SELECT DISTINCT UNNEST(contact_uids) AS id FROM {{ ref('int_brevo__contacts') }} WHERE est_interdit = TRUE OR date_di_rgpd_opposition IS NOT NULL
),

blocked_contact_uids AS (
    SELECT
        SPLIT_PART(id, ':', 3) AS id,
        SPLIT_PART(id, ':', 2) AS kind
    FROM blocked_contacts
),

blocked_services_uids AS (
    SELECT id FROM blocked_contact_uids WHERE kind = 'services'
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
        services.modes_orientation_beneficiaire          AS "modes_orientation_beneficiaire",
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
        services.thematiques                             AS "thematiques",
        services.types                                   AS "types",
        services.zone_diffusion_code                     AS "zone_diffusion_code",
        services.zone_diffusion_nom                      AS "zone_diffusion_nom",
        services.zone_diffusion_type                     AS "zone_diffusion_type",
        services.pre_requis                              AS "pre_requis",
        CASE
            WHEN blocked_services_uids.id IS NULL
                THEN services.contact_nom_prenom
        END                                              AS "contact_nom_prenom",
        CASE
            WHEN blocked_services_uids.id IS NULL
                THEN services.courriel
        END                                              AS "courriel",
        CASE
            WHEN blocked_services_uids.id IS NULL
                THEN services.telephone
        END                                              AS "telephone",
        ARRAY[services.frais]                            AS "frais"
    FROM services
    LEFT JOIN blocked_services_uids ON services.id = blocked_services_uids.id
)

SELECT * FROM final
