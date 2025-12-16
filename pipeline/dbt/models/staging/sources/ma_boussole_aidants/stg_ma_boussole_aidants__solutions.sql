-- https://docs.google.com/spreadsheets/d/1E_ya2wjJO9fvWbnngMcs9-FKshkzpgDrAkUSKslG3X4

SELECT
    x.code        AS "code",
    x.label       AS "label",
    x.description AS "description"
FROM (
    VALUES
    (
        1,
        'Premiers contacts',
        'Interlocuteur de premier niveau pour des conseils, orientation et accompagnement dans les démarches d’accès aux droits et aux aides financières. Ils sont en général la première porte d’entrée dans le parcours d’un aidant.'
    ),
    (
        2,
        'Démarches complémentaires',
        'Interlocuteur de second niveau pour toutes démarches de suivi de dossier (aides financières, congé proche aidant…)'
    ),
    (
        3,
        'Appui Juridique',
        'Interlocuteur apportant un soutien sur le plan juridique ou administratif, ou encore permettant la mise en place de la protection des majeurs (tutelle, curatelle).'
    ),
    (
        4,
        'Comprendre la situation de mon proche',
        'Information sur la maladie ou le handicap, orientation dans la recherche de solutions, de structures ou de professionnels spécialisés, accompagnement dans les démarches.'
    ),
    (
        5,
        'Soutien',
        'Ecoute et accompagnement, individuel ou collectif, avec un professionnel ou un bénévole, pour échanger sur sa situation.'
    ),
    (
        6,
        'Finances et patrimoine',
        'Acteurs renseignant sur les droits et devoirs relatifs aux impôts et patrimoine du proche aidé.'
    ),
    (
        7,
        'Aides à domicile',
        'Intervention d’un professionnel à domicile pour les tâches de la vie quotidienne (ménage, préparation des repas, courses, aide à la toilette…) ou pour les soins courants.'
    ),
    (
        8,
        'Portage de repas',
        'Livraison de repas préparés à domicile pour les personnes dépendantes.'
    ),
    (
        9,
        'Relais au domicile',
        'Intervention d’un professionnel à domicile, en relais du proche aidant, sur une période allant de quelques heures à plusieurs jours. Le service peut être nommé baluchonnage quand le relayeur assure une présence 24h/24 pendant plusieurs jours au domicile du proche dépendant.'
    ),
    (
        10,
        'Accompagnement à l’autonomie',
        'Accompagnement permettant de développer l’autonomie au quotidien (lieu de vie, courses, vie professionnelle, vie sociale, transports, vie affective et sexuelle…) '
    ),
    (
        11,
        'Soins à domicile',
        'Intervention de professionnels de santé - médecins, infirmiers, kinésithérapeutes…- à domicile pour dispenser les soins nécessaires.'
    ),
    (
        12,
        'Points d’information',
        'Interlocuteur de premier niveau pour des conseils, orientation et accompagnement dans les démarches d’aménagement du logement.'
    ),
    (
        13,
        'Diagnostic et adaptation du logement',
        'Acteur proposant une analyse et des recommandations d’adaptation du logement, permettant d’en optimiser le confort et la sécurité. La prestation pouvant aller jusqu’au suivi voire la réalisation des travaux d’aménagement.'
    ),
    (
        14,
        'Equipement',
        'Conseil, vente ou location d’équipements facilitant l’autonomie au domicile ou sur le lieu de travail.'
    ),
    (
        15,
        'Téléassistance',
        'Acteur pouvant conseiller sur des solutions de téléassistance, c’est-à-dire d’équipement permettant de contacter un téléopérateur en cas de chute ou de problème à domicile.'
    ),
    (
        16,
        'Services de transports',
        'Service assurant le transport des personnes en situation de handicap malades ou âgées.'
    ),
    (
        17,
        'Accompagnement aux déplacements',
        'Acteurs proposant des sorties accompagnées; de l’aide financière aux déplacements, ou encore de la formation à la mobilité en général.'
    ),
    (
        18,
        'Achat, location et adaptation de véhicule',
        'Acteurs proposant des véhicules adaptés ou aides techniques favorisant les déplacements des personnes à mobilité réduite : conseil, vente, location de matériel, ou encore aménagement de véhicule standard.'
    ),
    (
        19,
        'Formation à la conduite',
        'Auto-école ou centre de formation proposant une formation adaptée pour l’apprentissage ou le réapprentissage de la conduite.'
    ),
    (
        20,
        'Lieux de diagnostic',
        'Structure permettant, à la suite d’une série d’investigations, d’établir un diagnostic - une évaluation - sur les troubles rencontrés.'
    ),
    (
        21,
        'Rééducation et suivi thérapeutique',
        'Lieu regroupant plusieurs professionnels - soit pour aider la personne à progresser (orthophoniste, kinésithérapeute, ergothérapeute, psychologue), soit pour la soigner en prenant en compte ses besoins spécifiques (dermatologue, dentiste, gynécologue formés au handicap).'
    ),
    (
        23,
        'Prévention santé',
        'Bilan de prévention médico-psychologique, conférence ou atelier d’information et de sensibilisation sur des thèmes tels que la nutrition, le sommeil ou encore la mémoire, visant à prévenir les facteurs de risques pour la santé.'
    ),
    (
        24,
        'Soins palliatifs',
        'Soins continus visant à soulager la douleur et limiter la souffrance psychique, dispensés à domicile ou en établissement, par une équipe pluri-disciplinaire.'
    ),
    (
        25,
        'Soutien moral',
        'Ecoute et accompagnement, individuel ou collectif, pour être soutenu moralement et psychologiquement dans les différentes étapes du parcours d’aidant.'
    ),
    (
        26,
        'Formation de l’aidant',
        'Temps d’échange et de mise en pratique animés par un professionnel et/ou un bénévole formé, pour analyser et comprendre les situations vécues au quotidien et trouver des réponses adaptées.'
    ),
    (
        27,
        'Bien-être',
        'Activités favorisant la détente et le bien-être de l’aidant : sophrologie, yoga, méditation, art-thérapie, massages, zoothérapie...'
    ),
    (
        28,
        'Organisateurs de séjour',
        'Structure organisant des séjours de vacances en milieu ordinaire ou non, adaptés aux besoins spécifiques de la personne.'
    ),
    (
        29,
        'Lieux de séjour / vacances',
        'Séjour destiné à la personne et à ses proches aidants, offrant une prise en charge adaptée aux besoins de la personne accompagnée, ainsi que des activités différenciées et d’autres communes. '
    ),
    (
        30,
        'Convivialité',
        'Activité favorisant la vie sociale (jeu, moment convivial, discussion, visites pour les personnes isolées…)'
    ),
    (
        31,
        'Activités physiques',
        'Activité physique adaptée à l’âge et la situation de l’enfant ou de la personne.'
    ),
    (
        32,
        'Activités culturelles et artistiques',
        'Activité culturelle ou artistique adaptée à l’âge et la situation de la personne.'
    ),
    (
        34,
        'Activités de bien-être',
        'Activités favorisant la détente et le bien-être de l’aidé : sophrologie, yoga, méditation, art-thérapie, massages, zoothérapie...'
    ),
    (
        35,
        'Etablissements pour personnes en situation de handicap',
        'Accueil permanent où des professionnels assurent 24h sur 24 les soins et l’aide au quotidien des personnes en situation de handicap, en fonction de leurs besoins spécifiques.'
    ),
    (
        36,
        'Habitats alternatifs',
        'Habitat partagé ou participatif de type « co-propriété » par exemple. Ou encore habitat accompagné, de type « colocation ». Certaines sont intergénérationnelles. Une colocation peut se faire avec l’accompagnement de professionnels. L’habitat partagé ou accompagné peut être qualifié d’inclusif lorsqu’il comprend un projet de vie sociale.'
    ),
    (
        37,
        'Résidences pour personnes âgées autonomes',
        'Résidences pour personnes âgées, permettant de vivre en autonomie tout en maintenant une vie sociale et en mutualisant certains services.'
    ),
    (
        38,
        'Résidences pour personnes âgées dépendantes',
        'Accueil permanent pour les personnes âgées en perte d’autonomie ou non avec des services adaptés au besoin de la personne : du service à la carte, à l’aide 24h sur 24 pour les soins et la vie quotidienne.'
    ),
    (
        39,
        'Points d’information sur le répit',
        'Points d’information permettant d’orienter l’aidant vers des solutions de relais et de répit.'
    ),
    (
        40,
        'Relais au domicile',
        'Intervention d’un professionnel à domicile, en relais du proche aidant, sur une période allant de quelques heures à plusieurs jours.'
    ),
    (
        41,
        'Relais hors du domicile',
        'Accueil pour les personnes en perte d’autonomie ou handicapées pour une période de quelques heures, plusieurs jours ou plusieurs semaines.'
    ),
    (
        42,
        'Crèches, assistantes maternelles',
        'Solutions d’accueil pour les enfants qui ne vont pas encore à l’école. Certaines sont spécialement équipées et leur équipe est formée pour mieux accueillir les enfants avec un handicap. '
    ),
    (
        43,
        'Accueils de loisirs et péri-scolaires',
        'Solutions d’accueil pour les enfants et adolescents après l’école et pendant les congés scolaires. Certaines sont spécialement équipées et leur équipe est formée pour mieux accueillir les enfants avec un handicap. '
    ),
    (
        44,
        'Baby-sitting',
        'Solution de garde à domicile, qui propose des intervenants formés ou sensibilisés au handicap.'
    ),
    (
        45,
        'Scolarisation à l’école ordinaire',
        'Classes adaptées en école ordinaire, et solutions de soutien à la scolarité et d’accompagnement à l’autonomie pour les enfants handicapés.'
    ),
    (
        46,
        'Scolarisation en établissements spécialisés',
        'Etablissement spécialisé proposant des enseignements adaptés et des soins.'
    ),
    (
        48,
        'Aide aux étudiants',
        'Dispositifs spécifiques d’accueil, de logement, de restauration ou encore d’accompagnement permettant de poursuivre un cursus ordinaire de formation.'
    ),
    (
        49,
        'Formations, insertion professionnelle',
        'Accompagnement permettant de sécuriser le parcours de formation, d’accès ou de maintien en emploi d’une personne en situation de handicap : mise en situation en milieu professionnel, emploi accompagné…'
    ),
    (
        50,
        'Secteur protégé ou adapté',
        'Etablissements offrant des conditions de travail particulièrement aménagées permettant à des personnes handicapées de travailler.'
    ),
    (
        51,
        'Soins à domicile',
        'Intervention de professionnels de santé - médecins, infirmiers, kinésithérapeutes…- à domicile pour dispenser les soins nécessaires.'
    )
) AS x (code, label, description)
