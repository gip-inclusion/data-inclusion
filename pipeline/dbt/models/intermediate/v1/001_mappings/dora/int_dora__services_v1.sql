WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_dora__adresses_v1') }}
),

forms AS (
    SELECT
        services.id AS service_id,
        JSONB_AGG(
            JSONB_BUILD_OBJECT(
                'name', doc ->> 'nom',
                'url', doc ->> 'fichier'
            )
        )           AS "forms"
    FROM services
    CROSS JOIN JSONB_ARRAY_ELEMENTS(services.documents_a_completer) AS doc
    GROUP BY services.id
),

final AS (
    SELECT
        'dora'                                  AS "source",
        'dora--' || services.id                 AS "id",
        'dora--' || services.structure_id       AS "structure_id",
        adresses.id                             AS "adresse_id",
        services.lien_mobilisation              AS "lien_mobilisation",
        services.horaires_accueil               AS "horaires_accueil",
        services.lien_source                    AS "lien_source",
        services.conditions_acces               AS "conditions_acces",
        CAST(services.date_maj AS DATE)         AS "date_maj",
        CASE
            WHEN LENGTH(services.description) >= 10000
                THEN LEFT(services.description, 9999) || '…'
            ELSE services.description
        END                                     AS "description",
        services.thematiques                    AS "thematiques",
        services.modes_accueil                  AS "modes_accueil",
        services.modes_mobilisation             AS "modes_mobilisation",
        services.mobilisable_par                AS "mobilisable_par",
        services.mobilisation_precisions        AS "mobilisation_precisions",
        services.publics                        AS "publics",
        services.publics_precisions             AS "publics_precisions",
        services.type                           AS "type",
        services.frais                          AS "frais",
        services.frais_autres                   AS "frais_precisions",
        services.temps_passe_semaines           AS "nombre_semaines",
        services.temps_passe_duree_hebdomadaire AS "volume_horaire_hebdomadaire",
        services.zone_eligibilite               AS "zone_eligibilite",
        services.contact_nom_prenom             AS "contact_nom_prenom",
        services.courriel                       AS "courriel",
        services.telephone                      AS "telephone",
        services.contact_public                 AS "_is_contact_public",
        CASE
            WHEN LENGTH(services.nom) <= 150 THEN services.nom
            ELSE LEFT(services.nom, 149) || '…'
        END                                     AS "nom",
        JSONB_BUILD_OBJECT(
            'funding_labels', services.labels_financement,
            'forms', forms.forms,
            'online_form', services.formulaire_en_ligne_a_completer
        )                                       AS "_extra"
    FROM services
    LEFT JOIN adresses ON ('dora--' || services.id) = adresses.id
    LEFT JOIN forms ON services.id = forms.service_id
)

SELECT * FROM final
