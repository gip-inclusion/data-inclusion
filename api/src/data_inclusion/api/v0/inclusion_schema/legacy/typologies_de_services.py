from data_inclusion.schema.base import EnhancedEnum


class TypologieService(EnhancedEnum):
    ACCOMPAGNEMENT = (
        "accompagnement",
        "Accompagnement",
        "Etre accompagné·e dans ses démarches ou son retour à l’emploi durable",
    )
    ACCUEIL = (
        "accueil",
        "Accueil",
        """
            Lieux d’écoute et d’évaluation des besoins de la personne qui se présente
            afin de la guider vers le ou les organismes les plus susceptibles de
            répondre à ses besoins
        """,
    )
    AIDE_FINANCIERE = (
        "aide-financiere",
        "Aide financière",
        """
            Prise en charge financière, aide ou rémunération visant à aider une
            personne à accéder à un service
        """,
    )
    AIDE_MATERIELLE = (
        "aide-materielle",
        "Aide materielle",
        "Accéder à du matériel, voiture, vélo, ordinateur, alimentation",
    )
    ATELIER = (
        "atelier",
        "Atelier",
        "Etre accompagné·e dans sa montée en compétence sur un sujet spécifique",
    )
    FORMATION = (
        "formation",
        "Formation",
        "Formation dispensée par des professionnels pour les bénéficiaires",
    )
    INFORMATION = (
        "information",
        "Information",
        "Premier niveau d’information apporté par des professionnels du secteur",
    )
    NUMÉRIQUE = (
        "numerique",
        "Service numérique",
        "Service en ligne qui renvoi vers un site dédié",
    )
    AUTONOMIE = (
        "autonomie",
        "En autonomie",
        "Réaliser des démarches en toute autonomie",
    )
    DELEGATION = (
        "delegation",
        "Délégation",
        "Une personne fait les démarches à ma place.",
    )
    FINANCEMENT = (
        "financement",
        "Financement ",
        "Crédit solidaire, prêts à taux zéro pour le financement d’un projet",
    )
