{{ config(materialized='ephemeral') }}
SELECT
    x.category,
    x.thematique
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('access_to_housing',                     'logement-hebergement--etre-accompagne-pour-se-loger'),
    ('addiction',                             'sante--faire-face-a-une-situation-daddiction'),
    ('administrative_assistance',             'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives'),
    ('babysitting',                           'famille--garde-denfants'),
    ('budget_advice',                         'gestion-financiere--apprendre-a-gerer-son-budget'),
    ('carpooling',                            'mobilite'),
    ('chauffeur_driven_transport',            'mobilite'),
    ('clothing',                              'equipement-et-alimentation--habillement'),
    ('computers_at_your_disposal',            'numerique--acceder-a-du-materiel'),
    ('day_hosting',                           'remobilisation--lien-social'),
    ('digital_tools_training',                'numerique'),
    ('emergency_accommodation',               'logement-hebergement--mal-loges-sans-logis'),
    ('family_area',                           'famille--soutien-aux-familles'),
    ('food_distribution',                     'equipement-et-alimentation--alimentation'),
    ('food_packages',                         'equipement-et-alimentation--alimentation'),
    ('french_course',                         'apprendre-francais--suivre-formation'),
    ('legal_advice',                          'acces-aux-droits-et-citoyennete--accompagnement-juridique'),
    ('long_term_accomodation',                'logement-hebergement--etre-accompagne-pour-se-loger'),
    ('other_activities',                      'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('parent_assistance',                     'famille--soutien-a-la-parentalite'),
    ('provision_of_vehicles',                 'mobilite--louer-un-vehicule'),
    ('psychological_support',                 'sante--bien-etre-psychologique'),
    ('public_writer',                         'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives'),
    ('seated_catering',                       'equipement-et-alimentation--alimentation'),
    ('social_grocery_stores',                 'equipement-et-alimentation--alimentation'),
    ('solidarity_store',                      'equipement-et-alimentation--habillement'),
    ('std_testing',                           'sante--prevention-et-acces-aux-soins'),
    ('telephone_at_your_disposal',            'equipement-et-alimentation--acces-a-un-telephone-et-un-abonnement'),
    ('vaccination',                           'sante--prevention-et-acces-aux-soins'),
    ('wellness',                              'remobilisation--bien-etre'),
    ('wifi',                                  'numerique--acceder-a-une-connexion-internet')
    -- noqa: enable=layout.spacing
) AS x (category, thematique)
