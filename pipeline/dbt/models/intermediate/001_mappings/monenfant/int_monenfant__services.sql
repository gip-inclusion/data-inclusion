{% set presentation_resume_avip %}
'Soutenir l’accueil des jeunes enfants (zéro à trois ans) et favoriser l’accès à l’emploi des parents éloignés de l’emploi, notamment les mères isolées : tels sont les objectifs des crèches à vocation d’insertion professionnelle (AVIP).'
{% endset %}

{% set presentation_detail_avip %}
'Les crèches à vocation d’insertion professionnelle, dites crèches Avip, facilitent l’accès à l’emploi des parents de jeunes enfants, notamment les mères isolées, en accueillant leurs jeunes enfants. En effet, ce dispositif, créé par Pôle emploi et la Cnaf, propose aux demandeurs d’emploi un service ponctuel pour faire garder leurs enfants de 0 à 3 ans afin qu’ils puissent mener à bien leurs démarches de recherche d’emploi (entretien d’embauche et de sélection, formation, rendez-vous avec Pôle emploi, période d’essai, stage, création d’entreprise…).

Les crèches Avip accueillent les enfants comme toutes les autres crèches. Elles appliquent le même tarif et répondent aux mêmes exigences de qualité. Mais elles portent une attention particulière aux parents éloignés de l’emploi et volontaires pour s’engager dans une démarche active de recherche d’emploi. Un effort spécifique est fait pour les parents qui élèvent seuls leurs enfants et ceux qui vivent dans un quartier classé en zone dite «politique de la ville». Les crèches Avip s’adressent aussi aux parents de jeunes enfants qui ont des horaires de travail en décalé puisqu’elles proposent d’accueillir les enfants sur des plages horaires plus larges.

Les **crèches à vocation d’insertion professionnelle (AVIP)** ont une double mission :

- Réserver une place en crèche de jeunes enfants (zéro à trois ans) de parents sans emploi
- Les accompagner vers l’emploi ou la formation professionnelle'
{% endset %}

WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        structure_id                                                                                                AS "id",
        structure_id                                                                                                AS "adresse_id",
        NULL                                                                                                        AS "prise_rdv",
        description__modalites_tarifaires                                                                           AS "frais_autres",
        ARRAY['familles-enfants']                                                                                   AS "profils",
        NULL                                                                                                        AS "profils_precisions",
        structure_id                                                                                                AS "structure_id",
        _di_source_id                                                                                               AS "source",
        TRUE                                                                                                        AS "cumulable",
        NULL                                                                                                        AS "formulaire_en_ligne",
        service_commun__calendrier__jours_horaires_text                                                             AS "recurrence",
        CAST(NULL AS DATE)                                                                                          AS "date_creation",
        CAST(NULL AS DATE)                                                                                          AS "date_suspension",
        coordonnees__telephone                                                                                      AS "telephone",
        coordonnees__adresse_mail                                                                                   AS "courriel",
        FALSE                                                                                                       AS "contact_public",
        NULL                                                                                                        AS "contact_nom_prenom",
        modified_date                                                                                               AS "date_maj",
        'commune'                                                                                                   AS "zone_diffusion_type",
        NULL                                                                                                        AS "zone_diffusion_code",
        NULL                                                                                                        AS "zone_diffusion_nom",
        CAST(NULL AS TEXT [])                                                                                       AS "modes_orientation_accompagnateur", -- will be overridden after geocoding
        NULL                                                                                                        AS "modes_orientation_accompagnateur_autres", -- will be overridden after geocoding
        CAST(NULL AS TEXT [])                                                                                       AS "modes_orientation_beneficiaire",
        NULL                                                                                                        AS "modes_orientation_beneficiaire_autres",
        CAST(NULL AS TEXT [])                                                                                       AS "pre_requis",
        CAST(NULL AS TEXT [])                                                                                       AS "justificatifs",
        CASE WHEN service_commun__avip THEN 'Crèche À Vocation d’Insertion Professionnelle' ELSE structure_name END AS "nom",
        ARRAY['payant']                                                                                             AS "frais",
        ARRAY['famille--garde-denfants']                                                                            AS "thematiques",
        'https://monenfant.fr/que-recherchez-vous/mode-d-accueil/' || structure_id                                  AS "lien_source",
        ARRAY['accueil']                                                                                            AS "types",
        ARRAY['en-presentiel']                                                                                      AS "modes_accueil",
        CASE
            WHEN service_commun__avip THEN {{ presentation_resume_avip }}
            WHEN LENGTH(description__projet) <= 280 THEN description__projet
            ELSE LEFT(description__projet, 279) || '…'
        END                                                                                                         AS "presentation_resume",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN service_commun__avip THEN {{ presentation_detail_avip }} END,
                CASE WHEN description__projet IS NOT NULL THEN '## La structure :' || E'\n\n' || description__projet END,
                CASE WHEN description__conditions_admission IS NOT NULL THEN '## Les conditions d’admission :' || E'\n\n' || description__conditions_admission END,
                CASE WHEN description__modalites_inscription IS NOT NULL THEN '## Les modalités d’inscription :' || E'\n\n' || description__modalites_inscription END
            ],
            E'\n\n'
        )                                                                                                           AS "presentation_detail",
        coordonnees__site_internet                                                                                  AS "page_web"
    FROM creches
)

SELECT * FROM final
