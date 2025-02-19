WITH organismes_formateurs AS (
    SELECT * FROM {{ ref('stg_carif_oref__organismes_formateurs') }}
),

organismes_formateurs__contacts AS (
    SELECT * FROM {{ ref('stg_carif_oref__organismes_formateurs__contacts') }}
),

coordonnees AS (
    SELECT * FROM {{ ref('stg_carif_oref__coordonnees') }}
),

final AS (
    -- sort and distinct by numero to keep the first action with the same numero
    -- because there are several contacts available for the same organismes_formateurs.numero
    SELECT DISTINCT ON (organismes_formateurs.numero)
        organismes_formateurs.numero                              AS "id",
        coordonnees.hash_adresse                                  AS "adresse_id",
        organismes_formateurs.siret_formateur__siret              AS "siret",
        CAST(NULL AS BOOLEAN)                                     AS "antenne",
        NULL                                                      AS "rna",
        organismes_formateurs.raison_sociale_formateur            AS "nom",
        COALESCE(coordonnees.telfixe[0], coordonnees.portable[0]) AS "telephone",
        coordonnees.courriel                                      AS "courriel",
        coordonnees.web[0]                                        AS "site_web",
        'carif-oref'                                              AS "source",
        NULL                                                      AS "lien_source",
        NULL                                                      AS "horaires_ouverture",
        NULL                                                      AS "accessibilite",
        CAST(NULL AS TEXT [])                                     AS "labels_nationaux",
        CAST(NULL AS TEXT [])                                     AS "labels_autres",
        'OF'                                                      AS "typologie",
        NULL                                                      AS "presentation_resume",
        NULL                                                      AS "presentation_detail",
        CURRENT_DATE                                              AS "date_maj",
        ARRAY['apprendre-francais']                               AS "thematiques"
    FROM organismes_formateurs
    LEFT JOIN organismes_formateurs__contacts
        ON organismes_formateurs.numero = organismes_formateurs__contacts.numero_organisme_formateur
    LEFT JOIN coordonnees
        ON organismes_formateurs__contacts.hash_coordonnees = coordonnees.hash_
    -- TODO(vmttn): fine tune sort order to keep the most relevant action
    ORDER BY organismes_formateurs.numero
)

SELECT * FROM final
