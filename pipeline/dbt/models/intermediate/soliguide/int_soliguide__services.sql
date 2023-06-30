WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

services AS (
    SELECT * FROM {{ ref('stg_soliguide__services') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_soliguide__categories') }}
),

di_thematique_by_soliguide_categorie_code AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://docs.google.com/spreadsheets/d/1PtQ0_JRhHr37Ftjdjuv97FlHqZYWnmneOlDroi-4WhA/edit
        ('100', 'sante'),
        ('101', 'sante--faire-face-a-une-situation-daddiction'),
        ('102', 'sante--prevention-et-acces-aux-soins'),
        ('103', 'sante--bien-etre-psychologique'),
        ('401', 'acces-aux-droits-et-citoyennete--accompagnement-juridique'),
        ('501', 'numerique--acceder-a-du-materiel'),
        ('502', 'numerique--acceder-a-une-connexion-internet'),
        ('504', 'equipement-et-alimentation--acces-a-un-telephone-et-un-abonnement'),
        ('600', 'equipement-et-alimentation--alimentation'),
        ('601', 'equipement-et-alimentation--alimentation'),
        ('602', 'equipement-et-alimentation--alimentation'),
        ('603', 'equipement-et-alimentation--alimentation'),
        ('604', 'equipement-et-alimentation--alimentation'),
        ('703', 'famille--garde-denfants'),
        ('704', 'famille--soutien-a-la-parentalite'),
        ('804', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
        ('902', 'equipement-et-alimentation--habillement'),
        ('903', 'equipement-et-alimentation--habillement'),
        ('1200', 'mobilite'),
        ('1201', 'mobilite--comprendre-et-utiliser-les-transports-en-commun'),
        ('1202', 'mobilite--louer-un-vehicule'),
        ('1203', 'mobilite--comprendre-et-utiliser-les-transports-en-commun'),
        ('1300', 'logement-hebergement'),
        ('1301', 'logement-hebergement--mal-loges-sans-logis'),
        ('1302', 'logement-hebergement--mal-loges-sans-logis'),
        ('1303', 'logement-hebergement--mal-loges-sans-logis'),
        ('1305', 'logement-hebergement--etre-accompagne-pour-se-loger')

    -- Soliguide va retravailler ces catégories :
    -- ('1204', 'mobilite--aides-a-la-reprise-demploi-ou-a-la-formation'),

    ) AS x (categorie, thematique)
),

final AS (
    SELECT
        services.id                                              AS "id",
        lieux.lieu_id                                            AS "adresse_id",
        services._di_source_id                                   AS "source",
        NULL::TEXT []                                            AS "types",
        NULL                                                     AS "prise_rdv",
        NULL::TEXT []                                            AS "frais",
        NULL                                                     AS "frais_autres",
        NULL::TEXT []                                            AS "profils",
        NULL                                                     AS "pre_requis",
        NULL                                                     AS "cumulable",
        NULL                                                     AS "justificatifs",
        NULL                                                     AS "date_creation",
        NULL                                                     AS "date_suspension",
        NULL                                                     AS "telephone",
        NULL                                                     AS "courriel",
        NULL                                                     AS "contact_public",
        NULL                                                     AS "contact_nom_prenom",
        services.updated_at                                      AS "date_maj",
        NULL                                                     AS "zone_diffusion_type",
        NULL                                                     AS "zone_diffusion_code",
        NULL                                                     AS "zone_diffusion_nom",
        NULL                                                     AS "formulaire_en_ligne",
        NULL                                                     AS "recurrence",
        services.lieu_id                                         AS "structure_id",
        NULL::TEXT []                                            AS "modes_accueil",
        NULL::TEXT []                                            AS "modes_orientation_accompagnateur",
        NULL::TEXT []                                            AS "modes_orientation_beneficiaire",
        ARRAY(
            SELECT di_thematique_by_soliguide_categorie_code.thematique
            FROM di_thematique_by_soliguide_categorie_code
            WHERE services.categorie = di_thematique_by_soliguide_categorie_code.categorie
        )::TEXT []                                               AS "thematiques",
        categories.label || COALESCE(' : ' || services.name, '') AS "nom",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url        AS "lien_source",
        CASE LENGTH(services.description) <= 280
            WHEN TRUE THEN services.description
            WHEN FALSE THEN LEFT(services.description, 279) || '…'
        END                                                      AS "presentation_resume",
        CASE LENGTH(services.description) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN services.description
        END                                                      AS "presentation_detail"
    FROM services LEFT JOIN lieux ON services.lieu_id = lieux.id
    LEFT JOIN categories ON services.categorie = categories.code
    ORDER BY 1
)

SELECT * FROM final
