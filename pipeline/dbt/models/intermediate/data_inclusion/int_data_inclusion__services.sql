WITH services AS (
    SELECT * FROM {{ ref('stg_data_inclusion__services') }}
),

di_profil_by_dora_profil AS (
    -- dora's thematiques are not yet normalized
    SELECT x.*
    FROM (
        VALUES
        ('Adultes', 'adultes'),
        ('Femmes', 'femmes'),
        ('Public bénéficiaire du Revenu de Solidarité Active (RSA)', 'beneficiaire-rsa'),
        ('Demandeur d''emploi', 'demandeur-demploi')
    ) AS x (dora_profil, di_profil)
),

final AS (
    SELECT
        id                    AS "adresse_id",
        contact_public        AS "contact_public",
        NULL                  AS "contact_nom_prenom",
        NULL                  AS "courriel",
        cumulable             AS "cumulable",
        date_creation::DATE   AS "date_creation",
        date_maj::DATE        AS "date_maj",
        date_suspension::DATE AS "date_suspension",
        formulaire_en_ligne   AS "formulaire_en_ligne",
        frais_autres          AS "frais_autres",
        id                    AS "id",
        justificatifs         AS "justificatifs",
        NULL                  AS "lien_source",  -- ignored
        modes_accueil         AS "modes_accueil",
        NULL::TEXT []         AS "modes_orientation_accompagnateur",
        NULL::TEXT []         AS "modes_orientation_beneficiaire",
        nom                   AS "nom",
        presentation_resume   AS "presentation_resume",
        presentation_detail   AS "presentation_detail",
        prise_rdv             AS "prise_rdv",
        ARRAY(
            SELECT di_profil_by_dora_profil.di_profil
            FROM di_profil_by_dora_profil
            WHERE di_profil_by_dora_profil.dora_profil = ANY(services.profils)
        )::TEXT []            AS "profils",
        recurrence            AS "recurrence",
        _di_source_id         AS "source",
        structure_id          AS "structure_id",
        NULL                  AS "telephone",
        thematiques           AS "thematiques",
        types                 AS "types",
        zone_diffusion_code   AS "zone_diffusion_code",
        zone_diffusion_nom    AS "zone_diffusion_nom",
        zone_diffusion_type   AS "zone_diffusion_type",
        pre_requis            AS "pre_requis",
        ARRAY[frais]          AS "frais"
    FROM services
)

SELECT * FROM final
