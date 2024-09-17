WITH structures_garage AS (
    SELECT * FROM {{ ref('int_mes_aides__structures_garage') }}
),

structures_permis_velo AS (
    SELECT * FROM {{ ref('int_mes_aides__structures_permis_velo') }}
),

final AS (
    SELECT
        id,
        adresse_id,
        siret,
        antenne,
        rna,
        nom,
        telephone,
        courriel,
        site_web,
        "source",
        lien_source,
        horaires_ouverture,
        accessibilite,
        labels_nationaux,
        labels_autres,
        typologie,
        presentation_resume,
        presentation_detail,
        date_maj,
        thematiques
    FROM structures_garage
    UNION ALL
    SELECT
        id,
        adresse_id,
        siret,
        antenne,
        rna,
        nom,
        telephone,
        courriel,
        site_web,
        "source",
        lien_source,
        horaires_ouverture,
        accessibilite,
        labels_nationaux,
        labels_autres,
        typologie,
        presentation_resume,
        presentation_detail,
        date_maj,
        thematiques
    FROM structures_permis_velo
)

SELECT * FROM final
