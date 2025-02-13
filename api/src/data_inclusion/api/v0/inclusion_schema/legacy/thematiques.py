from data_inclusion.schema.base import EnhancedEnum


class Thematique(EnhancedEnum):
    ACCES_AUX_DROITS_ET_CITOYENNETE = (
        "acces-aux-droits-et-citoyennete",
        "Accès aux droits & citoyenneté",
        None,
    )
    ACCES_AUX_DROITS_ET_CITOYENNETE__ACCOMPAGNEMENT_DEMARCHES_ADMINISTRATIVES = (
        (
            "acces-aux-droits-et-citoyennete"
            "--accompagnement-dans-les-demarches-administratives"
        ),
        "Accompagnement dans les démarches administratives",
        None,
    )
    ACCES_AUX_DROITS_ET_CITOYENNETE__ACCOMPAGNEMENT_JURIDIQUE = (
        "acces-aux-droits-et-citoyennete--accompagnement-juridique",
        "Accompagnement juridique",
        None,
    )
    ACCES_AUX_DROITS_ET_CITOYENNETE__AIDE_AUX_VICTIMES = (
        "acces-aux-droits-et-citoyennete--aide-aux-victimes",
        "Aide aux victimes",
        None,
    )
    ACCES_AUX_DROITS_ET_CITOYENNETE__CONNAITRE_SES_DROITS = (
        "acces-aux-droits-et-citoyennete--connaitre-ses-droits",
        "Connaître ses droits",
        None,
    )
    ACCES_AUX_DROITS_ET_CITOYENNETE__DEMANDEURS_DASILE_ET_NATURALISATION = (
        "acces-aux-droits-et-citoyennete--demandeurs-dasile-et-naturalisation",
        "Demandeurs d’asile et naturalisation",
        None,
    )
    ACCES_AUX_DROITS_ET_CITOYENNETE__DEVELOPPEMENT_DURABLE = (
        "acces-aux-droits-et-citoyennete--developpement-durable",
        "Développement durable",
        None,
    )
    ACCES_AUX_DROITS_ET_CITOYENNETE__FACILITER_LACTION_CITOYENNE = (
        "acces-aux-droits-et-citoyennete--faciliter-laction-citoyenne",
        "Faciliter l’action citoyenne",
        None,
    )

    ACCOMPAGNEMENT_SOCIO_PRO_PERSONNALISE = (
        "accompagnement-social-et-professionnel-personnalise",
        "Accompagnement social et professionnel personnalisé",
        None,
    )
    ACCOMPAGNEMENT_SOCIO_PRO_PERSONNALISE__DECROCHAGE_SCOLAIRE = (
        "accompagnement-social-et-professionnel-personnalise--decrochage-scolaire",
        "Décrochage scolaire",
        None,
    )
    ACCOMPAGNEMENT_SOCIO_PRO_PERSONNALISE__DEFINITION_DU_PROJET_PROFESSIONNEL = (
        (
            "accompagnement-social-et-professionnel-personnalise--"
            "definition-du-projet-professionnel"
        ),
        "Définition du projet professionnel",
        None,
    )
    ACCOMPAGNEMENT_SOCIO_PRO_PERSONNALISE__PARCOURS_D_INSERTION_SOCIOPROFESSIONNEL = (
        (
            "accompagnement-social-et-professionnel-personnalise--"
            "parcours-d-insertion-socioprofessionnel"
        ),
        "Parcours d’insertion socio-professionnel",
        None,
    )

    APPRENDRE_FRANCAIS = (
        "apprendre-francais",
        "Apprendre le Français",
        None,
    )
    APPRENDRE_FRANCAIS__ACCOMPAGNEMENT_INSERTION_PRO = (
        "apprendre-francais--accompagnement-insertion-pro",
        "Accompagnement vers l’insertion professionnelle",
        None,
    )
    APPRENDRE_FRANCAIS__COMMUNIQUER_VIE_TOUS_LES_JOURS = (
        "apprendre-francais--communiquer-vie-tous-les-jours",
        "Communiquer dans la vie de tous les jours",
        None,
    )
    APPRENDRE_FRANCAIS__SUIVRE_FORMATION = (
        "apprendre-francais--suivre-formation",
        "Suivre une formation",
        None,
    )

    CHOISIR_UN_METIER = (
        "choisir-un-metier",
        "Choisir un métier",
        None,
    )
    CHOISIR_UN_METIER__CONFIRMER_SON_CHOIX_DE_METIER = (
        "choisir-un-metier--confirmer-son-choix-de-metier",
        "Confirmer son choix de métier",
        None,
    )
    CHOISIR_UN_METIER__CONNAITRE_LES_OPPORTUNITES_DEMPLOI = (
        "choisir-un-metier--connaitre-les-opportunites-demploi",
        "Connaître les opportunités d’emploi",
        None,
    )
    CHOISIR_UN_METIER__DECOUVRIR_UN_METIER_OU_UN_SECTEUR_DACTIVITE = (
        "choisir-un-metier--decouvrir-un-metier-ou-un-secteur-dactivite",
        "Découvrir un métier ou un secteur d’activité",
        None,
    )
    CHOISIR_UN_METIER__IDENTIFIER_SES_POINTS_FORTS_ET_SES_COMPETENCES = (
        "choisir-un-metier--identifier-ses-points-forts-et-ses-competences",
        "Identifier ses points forts et ses compétences",
        None,
    )

    CREATION_ACTIVITE = (
        "creation-activite",
        "Création d’activité",
        None,
    )
    CREATION_ACTIVITE__DEFINIR_SON_PROJET_DE_CREATION_DENTREPRISE = (
        "creation-activite--definir-son-projet-de-creation-dentreprise",
        "Définir son projet de création d’entreprise",
        None,
    )
    CREATION_ACTIVITE__DEVELOPPER_SON_ENTREPRISE = (
        "creation-activite--developper-son-entreprise",
        "Développer son entreprise",
        None,
    )
    CREATION_ACTIVITE__FINANCER_SON_PROJET = (
        "creation-activite--financer-son-projet",
        "Financer son projet",
        None,
    )
    CREATION_ACTIVITE__RESEAUTAGE_POUR_CREATEURS_DENTREPRISE = (
        "creation-activite--reseautage-pour-createurs-dentreprise",
        "Réseautage pour créateurs d’entreprise",
        None,
    )
    CREATION_ACTIVITE__STRUCTURER_SON_PROJET_DE_CREATION_DENTREPRISE = (
        "creation-activite--structurer-son-projet-de-creation-dentreprise",
        "Structurer son projet de création d’entreprise",
        None,
    )

    EQUIPEMENT_ET_ALIMENTATION = (
        "equipement-et-alimentation",
        "Équipement et alimentation",
        None,
    )
    EQUIPEMENT_ET_ALIMENTATION__ACCES_A_DU_MATERIEL_INFORMATIQUE = (
        "equipement-et-alimentation--acces-a-du-materiel-informatique",
        "Accès à du matériel informatique",
        None,
    )
    EQUIPEMENT_ET_ALIMENTATION__ACCES_A_UN_TELEPHONE_ET_UN_ABONNEMENT = (
        "equipement-et-alimentation--acces-a-un-telephone-et-un-abonnement",
        "Accès à un téléphone et un abonnement",
        None,
    )
    EQUIPEMENT_ET_ALIMENTATION__ALIMENTATION = (
        "equipement-et-alimentation--alimentation",
        "Alimentation",
        None,
    )
    EQUIPEMENT_ET_ALIMENTATION__AIDE_MENAGERE = (
        "equipement-et-alimentation--aide-menagere",
        "Aide ménagère",
        None,
    )
    EQUIPEMENT_ET_ALIMENTATION__ELECTROMENAGER = (
        "equipement-et-alimentation--electromenager",
        "Électroménager",
        None,
    )
    EQUIPEMENT_ET_ALIMENTATION__HABILLEMENT = (
        "equipement-et-alimentation--habillement",
        "Habillement",
        None,
    )

    FAMILLE = (
        "famille",
        "Famille",
        None,
    )
    FAMILLE__ACCOMPAGNEMENT_FEMME_ENCEINTE_BEBE_JEUNE_ENFANT = (
        "famille--accompagnement-femme-enceinte-bebe-jeune-enfant",
        "Accompagnement femme enceinte, bébé, jeune enfant",
        None,
    )
    FAMILLE__GARDE_DENFANTS = (
        "famille--garde-denfants",
        "Garde d’enfants",
        None,
    )
    FAMILLE__INFORMATION_ET_ACCOMPAGNEMENT_DES_PARENTS = (
        "famille--information-et-accompagnement-des-parents",
        "Information et accompagnement des parents",
        None,
    )
    FAMILLE__JEUNES_SANS_SOUTIEN_FAMILIAL = (
        "famille--jeunes-sans-soutien-familial",
        "Jeunes sans soutien familial",
        None,
    )
    FAMILLE__SOUTIEN_A_LA_PARENTALITE = (
        "famille--soutien-a-la-parentalite",
        "Soutien à la parentalité",
        None,
    )
    FAMILLE__SOUTIEN_AUX_FAMILLES = (
        "famille--soutien-aux-familles",
        "Soutien aux familles",
        None,
    )
    FAMILLE__VIOLENCES_INTRAFAMILIALES = (
        "famille--violences-intrafamiliales",
        "Violences intrafamiliales",
        None,
    )

    GESTION_FINANCIERE = (
        "gestion-financiere",
        "Gestion financière",
        None,
    )
    GESTION_FINANCIERE__ACCOMPAGNEMENT_AUX_PERSONNES_EN_DIFFICULTES_FINANCIERES = (
        "gestion-financiere--accompagnement-aux-personnes-en-difficultes-financieres",
        "Accompagnement aux personnes en difficultés financières",
        None,
    )
    GESTION_FINANCIERE__ACCES_AU_MICRO_CREDIT = (
        "gestion-financiere--acces-au-micro-credit",
        "Accès au micro-crédit",
        None,
    )
    GESTION_FINANCIERE__APPRENDRE_A_GERER_SON_BUDGET = (
        "gestion-financiere--apprendre-a-gerer-son-budget",
        "Apprendre à gérer son budget",
        None,
    )
    GESTION_FINANCIERE__BENEFICIER_DAIDES_FINANCIERES = (
        "gestion-financiere--beneficier-daides-financieres",
        "Bénéficier d’aides financières",
        None,
    )
    GESTION_FINANCIERE__CREATION_ET_UTILISATION_DUN_COMPTE_BANCAIRE = (
        "gestion-financiere--creation-et-utilisation-dun-compte-bancaire",
        "Création et utilisation d’un compte bancaire",
        None,
    )
    GESTION_FINANCIERE__OBTENIR_UNE_AIDE_ALIMENTAIRE = (
        "gestion-financiere--obtenir-une-aide-alimentaire",
        "Obtenir une aide alimentaire",
        None,
    )
    GESTION_FINANCIERE__PREVENTION_ET_GESTION_DU_SURENDETTEMENT = (
        "gestion-financiere--prevention-et-gestion-du-surendettement",
        "Prévention et gestion du surendettement",
        None,
    )

    HANDICAP = (
        "handicap",
        "Handicap",
        None,
    )
    HANDICAP__ACCOMPAGNEMENT_PAR_UNE_STRUCTURE_SPECIALISEE = (
        "handicap--accompagnement-par-une-structure-specialisee",
        "Accompagnement par une structure spécialisée",
        None,
    )
    HANDICAP__ADAPTATION_AU_POSTE_DE_TRAVAIL = (
        "handicap--adaptation-au-poste-de-travail",
        "Adaptation au poste de travail",
        None,
    )
    HANDICAP__ADAPTER_SON_LOGEMENT = (
        "handicap--adapter-son-logement",
        "Adapter son logement",
        None,
    )
    HANDICAP__AIDE_A_LA_PERSONNE = (
        "handicap--aide-a-la-personne",
        "Aide à la personne en situation de handicap ou malade",
        None,
    )
    HANDICAP__CONNAISSANCE_DES_DROITS_DES_TRAVAILLEURS = (
        "handicap--connaissance-des-droits-des-travailleurs",
        "Connaissance des droits des travailleurs",
        None,
    )
    HANDICAP__FAIRE_RECONNAITRE_UN_HANDICAP = (
        "handicap--faire-reconnaitre-un-handicap",
        "Faire reconnaître un handicap",
        None,
    )
    HANDICAP__FAVORISER_LE_RETOUR_ET_LE_MAINTIEN_DANS_LEMPLOI = (
        "handicap--favoriser-le-retour-et-le-maintien-dans-lemploi",
        "Favoriser le retour et le maintien dans l’emploi",
        None,
    )
    HANDICAP__GERER_LE_DEPART_A_LA_RETRAITE_DES_PERSONNES_EN_SITUATION_DE_HANDICAP = (
        (
            "handicap--"
            "gerer-le-depart-a-la-retraite-des-personnes-en-situation-de-handicap"
        ),
        "Gérer le départ à la retraite des personnes en situation de handicap",
        None,
    )
    HANDICAP__MOBILITE_DES_PERSONNES_EN_SITUATION_DE_HANDICAP = (
        "handicap--mobilite-des-personnes-en-situation-de-handicap",
        "Mobilité des personnes en situation de handicap",
        None,
    )

    ILLETRISME = (
        "illettrisme",
        "Illettrisme",
        None,
    )
    ILLETTRISME__ACCOMPAGNER_SCOLARITE = (
        "illettrisme--accompagner-scolarite",
        "Accompagner la scolarité d’un enfant",
        None,
    )
    ILLETTRISME__AMELIORER_VOCABULAIRE = (
        "illettrisme--ameliorer-vocabulaire",
        "Améliorer un niveau de vocabulaire",
        None,
    )
    ILLETTRISME__ETRE_AUTONOME = (
        "illettrisme--etre-autonome",
        "Être autonome dans la vie de tous les jours",
        None,
    )
    ILLETTRISME__INFO_ACQUISITION_CONNAISSANCES = (
        "illettrisme--info-acquisition-connaissances",
        "Être informé(e) sur l’acquisition des compétences de base",
        None,
    )
    ILLETTRISME__PERMIS_CONDUIRE = (
        "illettrisme--permis-conduire",
        "Passer le permis de conduire",
        None,
    )
    ILLETTRISME__REPERER_SITUATION_ILLETTRISME = (
        "illettrisme--reperer-situation-illettrisme",
        "Repérer des situations d’illettrisme",
        None,
    )
    ILLETTRISME__SURMONTER_TROUBLE_APPRENTISSAGE = (
        "illettrisme--surmonter-trouble-apprentissage",
        "Surmonter un trouble de l’apprentissage",
        None,
    )
    ILLETTRISME__TROUVER_EMPLOI_FORMATION = (
        "illettrisme--trouver-emploi-formation",
        "Trouver un emploi ou une formation",
        None,
    )
    ILLETTRISME__UTILISER_NUMERIQUE = (
        "illettrisme--utiliser-numerique",
        "Savoir utiliser les outils numériques",
        None,
    )
    ILLETTRISME__VALIDER_CERTIFICATION_CLEA = (
        "illettrisme--valider-certification-clea",
        "Valider une certification Cléa",
        None,
    )

    LOGEMENT_HEBERGEMENT = (
        "logement-hebergement",
        "Logement et hébergement",
        None,
    )
    LOGEMENT_HEBERGEMENT__AIDES_FINANCIERES_INVESTISSEMENT_LOCATIF = (
        "logement-hebergement--aides-financieres-investissement-locatif",
        "Aides financières pour l’investissement locatif",
        None,
    )
    LOGEMENT_HEBERGEMENT__BESOIN_DADAPTER_MON_LOGEMENT = (
        "logement-hebergement--besoin-dadapter-mon-logement",
        "Besoin d’adapter mon logement",
        None,
    )
    LOGEMENT_HEBERGEMENT__CONNAISSANCE_DE_SES_DROITS_ET_INTERLOCUTEURS = (
        "logement-hebergement--connaissance-de-ses-droits-et-interlocuteurs",
        "Connaissance de ses droits et interlocuteurs",
        None,
    )
    LOGEMENT_HEBERGEMENT__DEMENAGEMENT = (
        "logement-hebergement--demenagement",
        "Déménagement",
        None,
    )
    LOGEMENT_HEBERGEMENT__ETRE_ACCOMPAGNE_DANS_SON_PROJET_ACCESSION = (
        "logement-hebergement--etre-accompagne-dans-son-projet-accession",
        "Être accompagné(e) dans son projet d’accession",
        None,
    )
    LOGEMENT_HEBERGEMENT__ETRE_ACCOMPAGNE_EN_CAS_DE_DIFFICULTES_FINANCIERES = (
        "logement-hebergement--etre-accompagne-en cas-de-difficultes-financieres",
        "Être accompagné(e) en cas de difficultés financières",
        None,
    )
    LOGEMENT_HEBERGEMENT__ETRE_ACCOMPAGNE_POUR_SE_LOGER = (
        "logement-hebergement--etre-accompagne-pour-se-loger",
        "Être accompagné(e) pour se loger",
        None,
    )
    LOGEMENT_HEBERGEMENT__FINANCER_SON_PROJET_TRAVAUX = (
        "logement-hebergement--financer-son-projet-travaux",
        "Financer son projet de travaux",
        None,
    )
    LOGEMENT_HEBERGEMENT__GERER_SON_BUDGET = (
        "logement-hebergement--gerer-son-budget",
        "Gérer son budget",
        None,
    )
    LOGEMENT_HEBERGEMENT__MAL_LOGES_SANS_LOGIS = (
        "logement-hebergement--mal-loges-sans-logis",
        "Mal logé/sans logis",
        None,
    )
    LOGEMENT_HEBERGEMENT__PROBLEME_AVEC_SON_LOGEMENT = (
        "logement-hebergement--probleme-avec-son-logement",
        "Problème avec son logement",
        None,
    )
    LOGEMENT_HEBERGEMENT__REPRENDRE_UN_EMPLOI_OU_UNE_FORMATION = (
        "logement-hebergement--reprendre-un-emploi-ou-une-formation",
        "Reprendre un emploi ou une formation",
        None,
    )

    MOBILITE = (
        "mobilite",
        "Mobilité",
        None,
    )
    MOBILITE__ACHETER_UN_VEHICULE_MOTORISE = (
        "mobilite--acheter-un-vehicule-motorise",
        "Acheter un véhicule motorisé",
        None,
    )
    MOBILITE__ACHETER_UN_VELO = (
        "mobilite--acheter-un-velo",
        "Acheter un vélo",
        None,
    )
    MOBILITE__AIDES_A_LA_REPRISE_DEMPLOI_OU_A_LA_FORMATION = (
        "mobilite--aides-a-la-reprise-demploi-ou-a-la-formation",
        "Aides à la reprise d’emploi ou à la formation",
        None,
    )
    MOBILITE__APPRENDRE_A_UTILISER_UN_DEUX_ROUES = (
        "mobilite--apprendre-a-utiliser-un-deux-roues",
        "Apprendre à utiliser un deux roues",
        None,
    )
    MOBILITE__COMPRENDRE_ET_UTILISER_LES_TRANSPORTS_EN_COMMUN = (
        "mobilite--comprendre-et-utiliser-les-transports-en-commun",
        "Comprendre et utiliser les transports en commun",
        None,
    )
    MOBILITE__ENTRETENIR_REPARER_SON_VEHICULE = (
        "mobilite--entretenir-reparer-son-vehicule",
        "Entretenir ou réparer son véhicule",
        None,
    )
    MOBILITE__ETRE_ACCOMPAGNE_DANS_SON_PARCOURS_MOBILITE = (
        "mobilite--etre-accompagne-dans-son-parcours-mobilite",
        "Être accompagné(e) dans son parcours mobilité",
        None,
    )
    MOBILITE__LOUER_UN_VEHICULE = (
        "mobilite--louer-un-vehicule",
        "Louer un véhicule (voiture, vélo, scooter..)",
        None,
    )
    MOBILITE__FINANCER_MON_PROJET_MOBILITE = (
        "mobilite--financer-mon-projet-mobilite",
        "Financer mon projet mobilité",
        None,
    )
    MOBILITE__PREPARER_SON_PERMIS_DE_CONDUIRE_SE_REENTRAINER_A_LA_CONDUITE = (
        "mobilite--preparer-son-permis-de-conduire-se-reentrainer-a-la-conduite",
        "Préparer son permis de conduire, se réentraîner à la conduite",
        None,
    )

    NUMERIQUE = (
        "numerique",
        "Numérique",
        None,
    )
    NUMERIQUE__ACCEDER_A_DU_MATERIEL = (
        "numerique--acceder-a-du-materiel",
        "Accéder à du matériel",
        None,
    )
    NUMERIQUE__ACCEDER_A_UNE_CONNEXION_INTERNET = (
        "numerique--acceder-a-une-connexion-internet",
        "Accéder à une connexion internet",
        None,
    )
    NUMERIQUE__ACCOMPAGNER_LES_DEMARCHES_DE_SANTE = (
        "numerique--accompagner-les-demarches-de-sante",
        "Accompagner les démarches de santé",
        None,
    )
    NUMERIQUE__APPROFONDIR_MA_CULTURE_NUMERIQUE = (
        "numerique--approfondir-ma-culture-numerique",
        "Approfondir ma culture numérique",
        None,
    )
    NUMERIQUE__CREER_AVEC_LE_NUMERIQUE = (
        "numerique--creer-avec-le-numerique",
        "Créer avec le numérique",
        None,
    )
    NUMERIQUE__CREER_ET_DEVELOPPER_MON_ENTREPRISE = (
        "numerique--creer-et-developper-mon-entreprise",
        "Créer et développer mon entreprise",
        None,
    )
    NUMERIQUE__DEVENIR_AUTONOME_DANS_LES_DEMARCHES_ADMINISTRATIVES = (
        "numerique--devenir-autonome-dans-les-demarches-administratives",
        "Devenir autonome dans les démarches administratives",
        None,
    )
    NUMERIQUE__FAVORISER_MON_INSERTION_PROFESSIONNELLE = (
        "numerique--favoriser-mon-insertion-professionnelle",
        "Favoriser mon insertion professionnelle",
        None,
    )
    NUMERIQUE__PRENDRE_EN_MAIN_UN_ORDINATEUR = (
        "numerique--prendre-en-main-un-ordinateur",
        "Prendre en main un ordinateur",
        None,
    )
    NUMERIQUE__PRENDRE_EN_MAIN_UN_SMARTPHONE_OU_UNE_TABLETTE = (
        "numerique--prendre-en-main-un-smartphone-ou-une-tablette",
        "Prendre en main un smartphone ou une tablette",
        None,
    )
    NUMERIQUE__PROMOUVOIR_LA_CITOYENNETE_NUMERIQUE = (
        "numerique--promouvoir-la-citoyennete-numerique",
        "Promouvoir la citoyenneté numérique",
        None,
    )
    NUMERIQUE__REALISER_DES_DEMARCHES_ADMINISTRATIVES_AVEC_UN_ACCOMPAGNEMENT = (
        "numerique--realiser-des-demarches-administratives-avec-un-accompagnement",
        "Réaliser des démarches administratives avec un accompagnement",
        None,
    )
    NUMERIQUE__S_EQUIPER_EN_MATERIEL_INFORMATIQUE = (
        "numerique--s-equiper-en-materiel-informatique",
        "S’équiper en matériel informatique",
        None,
    )
    NUMERIQUE__SOUTENIR_LA_PARENTALITE_ET_L_EDUCATION_AVEC_LE_NUMERIQUE = (
        "numerique--soutenir-la-parentalite-et-l-education-avec-le-numerique",
        "Soutenir la parentalité et l’éducation avec le numérique",
        None,
    )
    NUMERIQUE__UTILISER_LE_NUMERIQUE_AU_QUOTIDIEN = (
        "numerique--utiliser-le-numerique-au-quotidien",
        "Utiliser le numérique au quotidien",
        None,
    )

    PREPARER_SA_CANDIDATURE = (
        "preparer-sa-candidature",
        "Préparer sa candidature",
        None,
    )
    PREPARER_SA_CANDIDATURE__DEVELOPPER_SON_RESEAU = (
        "preparer-sa-candidature--developper-son-reseau",
        "Développer son réseau",
        None,
    )
    PREPARER_SA_CANDIDATURE__ORGANISER_SES_DEMARCHES_DE_RECHERCHE_DEMPLOI = (
        "preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi",
        "Organiser ses démarches de recherche d’emploi",
        None,
    )
    PREPARER_SA_CANDIDATURE__REALISER_UN_CV_ET_OU_UNE_LETTRE_DE_MOTIVATION = (
        "preparer-sa-candidature--realiser-un-cv-et-ou-une-lettre-de-motivation",
        "Réaliser un CV et/ou une lettre de motivation",
        None,
    )
    PREPARER_SA_CANDIDATURE__VALORISER_SES_COMPETENCES = (
        "preparer-sa-candidature--valoriser-ses-competences",
        "Valoriser ses compétences",
        None,
    )

    REMOBILISATION = (
        "remobilisation",
        "Remobilisation",
        None,
    )
    REMOBILISATION__BIEN_ETRE = (
        "remobilisation--bien-etre",
        "Bien être",
        None,
    )
    REMOBILISATION__DECOUVRIR_SON_POTENTIEL_VIA_LE_SPORT_ET_LA_CULTURE = (
        "remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture",
        "Découvrir son potentiel via le sport et la culture",
        None,
    )
    REMOBILISATION__DISCRIMINATION = (
        "remobilisation--discrimination",
        "Discrimination",
        None,
    )
    REMOBILISATION__IDENTIFIER_SES_COMPETENCES_ET_APTITUDES = (
        "remobilisation--identifier-ses-competences-et-aptitudes",
        "Identifier ses compétences et aptitudes",
        None,
    )
    REMOBILISATION__LIEN_SOCIAL = (
        "remobilisation--lien-social",
        "Lien social",
        None,
    )
    REMOBILISATION__PARTICIPER_A_DES_ACTIONS_SOLIDAIRES_OU_DE_BÉNÉVOLAT = (
        "remobilisation--participer-a-des-actions-solidaires-ou-de-benevolat",
        "Participer à des actions solidaires ou de bénévolat",
        None,
    )
    REMOBILISATION__PRESSION_SOCIALE = (
        "remobilisation--pression-sociale",
        "Pression sociale",
        None,
    )
    REMOBILISATION__RESTAURER_SA_CONFIANCE_SON_IMAGE_DE_SOI = (
        "remobilisation--restaurer-sa-confiance-son-image-de-soi",
        "Restaurer sa confiance, son image de soi",
        None,
    )

    SANTE = (
        "sante",
        "Santé",
        None,
    )
    SANTE__ACCES_AUX_SOINS = (
        "sante--acces-aux-soins",
        "Accès aux soins",
        None,
    )
    SANTE__ACCOMPAGNEMENT_DE_LA_FEMME_ENCEINTE_DU_BEBE_ET_DU_JEUNE_ENFANT = (
        "sante--accompagnement-de-la-femme-enceinte-du-bebe-et-du-jeune-enfant",
        "Accompagnement de la femme enceinte, du bébé et du jeune enfant",
        None,
    )
    SANTE__ACCOMPAGNER_LES_TRAUMATISMES = (
        "sante--accompagner-les-traumatismes",
        "Accompagner les traumatismes",
        None,
    )
    SANTE__BIEN_ETRE_PSYCHOLOGIQUE = (
        "sante--bien-etre-psychologique",
        "Bien être psychologique",
        None,
    )
    SANTE__DIAGNOSTIC_ET_ACCOMPAGNEMENT_A_LEMPLOYABILITE = (
        "sante--diagnostic-et-accompagnement-a-lemployabilite",
        "Diagnostic et accompagnement à l’employabilité",
        None,
    )
    SANTE__FAIRE_FACE_A_UNE_SITUATION_DADDICTION = (
        "sante--faire-face-a-une-situation-daddiction",
        "Faire face à une situation d’addiction",
        None,
    )
    SANTE__OBTENIR_LA_PRISE_EN_CHARGE_DE_FRAIS_MEDICAUX = (
        "sante--obtenir-la-prise-en-charge-de-frais-medicaux",
        "Obtenir la prise en charge de frais médicaux",
        None,
    )
    SANTE__PREVENTION_ET_ACCES_AUX_SOINS = (
        "sante--prevention-et-acces-aux-soins",
        (
            "Prévention et accès aux soins "
            "(vaccination, éducation à la santé, lutte contre la tuberculose…)."
        ),
        None,
    )
    SANTE__SE_SOIGNER_ET_PRÉVENIR_LA_MALADIE = (
        "sante--se-soigner-et-prevenir-la-maladie",
        "Se soigner et prévenir la maladie",
        None,
    )
    SANTE__VIE_RELATIONNELLE_ET_AFFECTIVE = (
        "sante--vie-relationnelle-et-affective",
        "Vie relationnelle et affective, dépistage et prévention des IST/VIH…",
        None,
    )

    SE_FORMER = (
        "se-former",
        "Se former",
        None,
    )
    SE_FORMER__MONTER_SON_DOSSIER_DE_FORMATION = (
        "se-former--monter-son-dossier-de-formation",
        "Monter son dossier de formation",
        None,
    )
    SE_FORMER__TROUVER_SA_FORMATION = (
        "se-former--trouver-sa-formation",
        "Trouver sa formation",
        None,
    )
    SE_FORMER__UTILISER_LE_NUMÉRIQUE = (
        "se-former--utiliser-le-numerique",
        "Utiliser le numérique",
        None,
    )

    SOUVRIR_A_L_INTERNATIONAL = (
        "souvrir-a-linternational",
        "S’ouvrir à l’international",
        None,
    )
    SOUVRIR_A_L_INTERNATIONAL__CONNAITRE_LES_OPPORTUNITES_DEMPLOI_A_LETRANGER = (
        "souvrir-a-linternational--connaitre-les-opportunites-demploi-a-letranger",
        "Connaître les opportunités d’emploi à l’étranger",
        None,
    )
    SOUVRIR_A_L_INTERNATIONAL__SINFORMER_SUR_LES_AIDES_POUR_TRAVAILLER_A_LETRANGER = (
        "souvrir-a-linternational--sinformer-sur-les-aides-pour-travailler-a-letranger",
        "S’informer sur les aides pour travailler à l’étranger",
        None,
    )
    SOUVRIR_A_L_INTERNATIONAL__SORGANISER_SUITE_A_SON_RETOUR_EN_FRANCE = (
        "souvrir-a-linternational--sorganiser-suite-a-son-retour-en-france",
        "S’organiser suite à son retour en France",
        None,
    )

    TROUVER_UN_EMPLOI = (
        "trouver-un-emploi",
        "Trouver un emploi",
        None,
    )
    TROUVER_UN_EMPLOI__CONVAINCRE_UN_RECRUTEUR_EN_ENTRETIEN = (
        "trouver-un-emploi--convaincre-un-recruteur-en-entretien",
        "Convaincre un recruteur en entretien",
        None,
    )
    TROUVER_UN_EMPLOI__FAIRE_DES_CANDIDATURES_SPONTANEES = (
        "trouver-un-emploi--faire-des-candidatures-spontanees",
        "Faire des candidatures spontanées",
        None,
    )
    TROUVER_UN_EMPLOI__REPONDRE_A_DES_OFFRES_DEMPLOI = (
        "trouver-un-emploi--repondre-a-des-offres-demploi",
        "Répondre à des offres d’emploi",
        None,
    )
    TROUVER_UN_EMPLOI__SUIVRE_SES_CANDIDATURES_ET_RELANCER_LES_EMPLOYEURS = (
        "trouver-un-emploi--suivre-ses-candidatures-et-relancer-les-employeurs",
        "Suivre ses candidatures et relancer les employeurs",
        None,
    )
