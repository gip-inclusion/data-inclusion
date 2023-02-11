{{
    config(
        post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
    )
}}

WITH siaes AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__siaes') }}
),

organisations AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

structures AS (
    SELECT * FROM siaes
    UNION
    SELECT * FROM organisations
),

final AS (
    SELECT
        id,
        siret,
        antenne,
        rna,
        nom,
        commune,
        code_postal,
        code_insee,
        adresse,
        complement_adresse,
        longitude,
        latitude,
        telephone,
        courriel,
        site_web,
        'emplois-de-linclusion' AS source,
        lien_source,
        horaires_ouverture,
        accessibilite,
        labels_nationaux,
        labels_autres,
        thematiques,
        typologie,
        presentation_resume,
        presentation_detail,
        date_maj
    FROM structures
)

SELECT * FROM final
