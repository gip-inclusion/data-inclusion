WITH fredo_structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

fredo_services AS (SELECT * FROM {{ ref('stg_fredo__services') }}),

fredo_categories AS (SELECT * FROM {{ ref('stg_fredo__categories') }}),

fredo_frais AS (SELECT * FROM {{ ref('stg_fredo__frais') }}),

fredo_publics AS (SELECT * FROM {{ ref('stg_fredo__publics') }}),

map_thematiques AS (SELECT * FROM {{ ref('_map_fredo_thematiques') }}),

di_types_by_fredo_services AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://www.notion.so/dora-beta/8c8d883758df47649902ddba55ab2236?v=56c7542b590c42febc79db8d3ffe9f81&p=84e6e41b6a3b4f54bee46e11aa41ff47&pm=s
        ('accès aux droits', 'accompagnement'),
        ('accompagnement dans les démarches', 'accompagnement'),
        ('accompagnement dans les démarches via le numérique', 'accompagnement'),
        ('accompagnement global', 'accompagnement'),
        ('accompagnement psychologique', 'accompagnement'),
        ('activités / ateliers', 'atelier'),
        ('aide à la personne', 'accompagnement'),
        ('aide financière', 'aide-financiere'),
        ('aide logistique', 'aide-materielle'),
        ('dispositif', 'accompagnement'),
        ('ecoute / soutien', 'accueil'),
        ('information / orientation', 'information'),
        ('initiation', 'information'),
        ('soins', 'accompagnement')
    ) AS x (service_fredo, di_type)
),

di_profils_by_fredo_public AS (
    SELECT x.*
    FROM (
        VALUES
        ('adulte', 'adultes'),
        ('détenu ou sortant de détention', 'sortants-de-detention'),
        ('enfant', 'familles-enfants'),
        ('etudiant', 'etudiants'),
        ('femme', 'femmes'),
        ('jeune', 'jeunes'),
        ('personne âgée', 'seniors-65'),
        ('personne de nationalité étrangère', 'personnes-de-nationalite-etrangere'),
        ('sans domicile fixe', 'sans-domicile-fixe'),
        ('sortant ase', 'familles-enfants'),
        ('victime', 'victimes')
    ) AS x (public, profil)
),

di_frais_by_fredo_frais AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://www.notion.so/dora-beta/8c8d883758df47649902ddba55ab2236?v=56c7542b590c42febc79db8d3ffe9f81&p=84e6e41b6a3b4f54bee46e11aa41ff47&pm=s
        ('gratuit', 'gratuit'),
        ('payant', 'payant'),
        ('gratuit sous condition', 'gratuit-sous-conditions'),
        ('gratuit sous conditions', 'gratuit-sous-conditions'),
        ('sous conditions', 'gratuit-sous-conditions')
    ) AS x (frais_fredo, frais_di)
),

di_mode_by_fredo_frais_di AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://www.notion.so/dora-beta/8c8d883758df47649902ddba55ab2236?v=56c7542b590c42febc79db8d3ffe9f81&p=84e6e41b6a3b4f54bee46e11aa41ff47&pm=s
        ('sans rendez-vous', 'se-presenter'),
        ('sur rendez-vous', 'envoyer-un-mail'),
        ('sur rendez-vous', 'telephoner'),
        ('sur rendez-vous', 'prendre-rdv'),
        ('avec rendez-vous', 'envoyer-un-mail'),
        ('avec rendez-vous', 'telephoner'),
        ('avec rendez-vous', 'prendre-rdv')
    ) AS x (frais_fredo, mode_benef_di)
),

frais AS (
    SELECT
        fredo_frais.structure_id,
        ARRAY_AGG(di_frais_by_fredo_frais.frais_di) AS frais
    FROM fredo_frais
    INNER JOIN di_frais_by_fredo_frais ON fredo_frais.value = di_frais_by_fredo_frais.frais_fredo
    GROUP BY fredo_frais.structure_id
),

frais_autres AS (
    SELECT
        fredo_frais.structure_id,
        STRING_AGG(fredo_frais.value, ', ') AS frais_autres
    FROM fredo_frais
    WHERE fredo_frais.value NOT IN (SELECT frais_fredo FROM di_frais_by_fredo_frais)
    GROUP BY fredo_frais.structure_id
),

mode_orient_benef AS (
    SELECT
        fredo_frais.structure_id,
        ARRAY_AGG(DISTINCT di_mode_by_fredo_frais_di.mode_benef_di) AS mode_orient_benef
    FROM fredo_frais
    INNER JOIN di_mode_by_fredo_frais_di ON fredo_frais.value = di_mode_by_fredo_frais_di.frais_fredo
    GROUP BY fredo_frais.structure_id
),

thematiques AS (
    SELECT
        fredo_categories.structure_id,
        ARRAY_AGG(DISTINCT map_thematiques.thematiques) AS thematiques
    FROM fredo_categories
    INNER JOIN map_thematiques ON fredo_categories.value = map_thematiques.category
    GROUP BY fredo_categories.structure_id
),

profils AS (
    SELECT
        fredo_publics.structure_id,
        ARRAY_AGG(di_profils_by_fredo_public.profil) AS profils
    FROM fredo_publics
    INNER JOIN di_profils_by_fredo_public ON fredo_publics.value = di_profils_by_fredo_public.public
    GROUP BY fredo_publics.structure_id
),

final AS (
    SELECT
        fredo_structures.id                                                                   AS "structure_id",
        fredo_structures._di_source_id                                                        AS "source",
        fredo_structures.id                                                                   AS "adresse_id",
        thematiques.thematiques                                                               AS "thematiques",
        NULL                                                                                  AS "prise_rdv",
        NULL                                                                                  AS "page_web",
        frais.frais                                                                           AS "frais",
        profils.profils                                                                       AS "profils",
        NULL                                                                                  AS "modes_orientation_accompagnateur_autres",
        NULL                                                                                  AS "modes_orientation_beneficiaire_autres",
        NULL                                                                                  AS "formulaire_en_ligne",
        fredo_structures.lien_source                                                          AS "lien_source",
        fredo_structures.telephone                                                            AS "telephone",
        NULL                                                                                  AS "courriel",
        NULL                                                                                  AS "contact_nom_prenom",
        'departement'                                                                         AS "zone_diffusion_type",
        '974'                                                                                 AS "zone_diffusion_code",
        'La Réunion'                                                                          AS "zone_diffusion_nom",
        NULL                                                                                  AS "recurrence",
        fredo_structures.last_update                                                          AS "date_maj",
        CAST(UUID(MD5(fredo_structures.id || COALESCE(fredo_services.value, ''))) AS TEXT)    AS "id",
        CAST(NULL AS TEXT [])                                                                 AS "pre_requis",
        CAST(NULL AS BOOLEAN)                                                                 AS "cumulable",
        CAST(NULL AS TEXT [])                                                                 AS "justificatifs",
        CAST(NULL AS DATE)                                                                    AS "date_creation",
        CAST(NULL AS DATE)                                                                    AS "date_suspension",
        CAST(NULL AS BOOLEAN)                                                                 AS "contact_public",
        CAST(NULL AS TEXT [])                                                                 AS "modes_accueil",
        LEFT(fredo_structures.presentation_resume, 280)                                       AS "presentation_resume",
        CASE
            WHEN CHAR_LENGTH(fredo_structures.presentation_resume) > 280
                THEN fredo_structures.presentation_resume
        END                                                                                   AS "presentation_detail",
        ARRAY[(
            SELECT di_types_by_fredo_services.di_type
            FROM di_types_by_fredo_services
            WHERE fredo_services.value = di_types_by_fredo_services.service_fredo
        )]                                                                                    AS "types",
        (
            SELECT REPLACE((UPPER(LEFT(di_types_by_fredo_services.service_fredo, 1)) || SUBSTRING(di_types_by_fredo_services.service_fredo FROM 2)), '/', 'et')
            FROM di_types_by_fredo_services
            WHERE fredo_services.value = di_types_by_fredo_services.service_fredo
        )                                                                                     AS "nom",
        CASE
            WHEN frais.frais IS NULL THEN frais_autres.frais_autres
        END                                                                                   AS "frais_autres",
        ARRAY['envoyer-un-mail', 'telephoner']                                                AS "modes_orientation_accompagnateur",
        COALESCE(mode_orient_benef.mode_orient_benef, ARRAY['envoyer-un-mail', 'telephoner']) AS "modes_orientation_beneficiaire"
    FROM fredo_services
    LEFT JOIN fredo_structures ON fredo_services.structure_id = fredo_structures.id
    LEFT JOIN thematiques ON fredo_structures.id = thematiques.structure_id
    LEFT JOIN profils ON fredo_structures.id = profils.structure_id
    LEFT JOIN frais ON fredo_structures.id = frais.structure_id
    LEFT JOIN frais_autres ON fredo_structures.id = frais_autres.structure_id
    LEFT JOIN mode_orient_benef ON fredo_structures.id = mode_orient_benef.structure_id
)

SELECT * FROM final
