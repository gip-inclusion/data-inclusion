WITH services_garage AS (
    SELECT * FROM {{ ref('int_mes_aides__services_garage') }}
),

services_permis_velo AS (
    SELECT * FROM {{ ref('int_mes_aides__services_permis_velo') }}
),

final AS (
    SELECT
        adresse_id,
        contact_public,
        cumulable,
        date_creation,
        date_maj,
        date_suspension,
        formulaire_en_ligne,
        frais_autres,
        id,
        justificatifs,
        lien_source,
        modes_accueil,
        modes_orientation_accompagnateur,
        modes_orientation_accompagnateur_autres,
        modes_orientation_beneficiaire,
        modes_orientation_beneficiaire_autres,
        nom,
        presentation_resume,
        presentation_detail,
        prise_rdv,
        profils,
        recurrence,
        source,
        structure_id,
        thematiques,
        types,
        zone_diffusion_code,
        zone_diffusion_nom,
        zone_diffusion_type,
        pre_requis,
        contact_nom_prenom,
        courriel,
        telephone,
        frais,
        page_web
    FROM services_garage
    UNION ALL
    SELECT
        adresse_id,
        contact_public,
        cumulable,
        date_creation,
        date_maj,
        date_suspension,
        formulaire_en_ligne,
        frais_autres,
        id,
        justificatifs,
        lien_source,
        modes_accueil,
        modes_orientation_accompagnateur,
        modes_orientation_accompagnateur_autres,
        modes_orientation_beneficiaire,
        modes_orientation_beneficiaire_autres,
        nom,
        presentation_resume,
        presentation_detail,
        prise_rdv,
        profils,
        recurrence,
        source,
        structure_id,
        thematiques,
        types,
        zone_diffusion_code,
        zone_diffusion_nom,
        zone_diffusion_type,
        pre_requis,
        contact_nom_prenom,
        courriel,
        telephone,
        frais,
        page_web
    FROM services_permis_velo
)

SELECT * FROM final
