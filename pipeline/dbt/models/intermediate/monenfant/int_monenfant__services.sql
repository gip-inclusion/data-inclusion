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
        id                                                                                 AS "id",
        id                                                                                 AS "adresse_id",
        NULL                                                                               AS "prise_rdv",
        NULL                                                                               AS "frais_autres",
        NULL::TEXT []                                                                      AS "profils",
        id                                                                                 AS "structure_id",
        _di_source_id                                                                      AS "source",
        NULL                                                                               AS "pre_requis",
        TRUE                                                                               AS "cumulable",
        NULL                                                                               AS "justificatifs",
        NULL                                                                               AS "formulaire_en_ligne",
        details_infos_pratiques_jour_horaire                                               AS "recurrence",
        NULL::DATE                                                                         AS "date_creation",
        NULL::DATE                                                                         AS "date_suspension",
        telephone                                                                          AS "telephone",
        mail                                                                               AS "courriel",
        FALSE                                                                              AS "contact_public",
        NULL                                                                               AS "contact_nom_prenom",
        derniere_modif_date                                                                AS "date_maj",
        'commune'                                                                          AS "zone_diffusion_type",
        NULL                                                                               AS "zone_diffusion_code", -- will be overridden after geocoding
        NULL                                                                               AS "zone_diffusion_nom", -- will be overridden after geocoding
        NULL::TEXT []                                                                      AS "modes_orientation_accompagnateur",
        NULL::TEXT []                                                                      AS "modes_orientation_beneficiaire",
        CASE WHEN avip THEN 'Crèches À Vocation d''Insertion Professionnelle' ELSE nom END AS "nom",
        ARRAY['payant']                                                                    AS "frais",
        ARRAY['famille--garde-denfants']                                                   AS "thematiques",
        'https://monenfant.fr/que-recherchez-vous/' || result_id                           AS "lien_source",
        ARRAY['accueil']                                                                   AS "types",
        ARRAY['en-presentiel']                                                             AS "modes_accueil",
        CASE
            WHEN avip THEN {{ presentation_resume_avip }}
            ELSE {{ truncate_text("details_presentation_structure_projet") }}
        END                                                                                AS "presentation_resume",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN avip THEN {{ presentation_detail_avip }} END,
                details_presentation_structure_projet,
                details_modalite_condition_admision,
                details_modalite_modalites_inscription
            ],
            E'\n\n'
        )                                                                                  AS "presentation_detail"
    FROM creches
)

SELECT * FROM final
