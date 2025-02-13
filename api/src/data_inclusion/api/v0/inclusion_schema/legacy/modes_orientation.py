from data_inclusion.schema.base import EnhancedEnum


class ModeOrientationAccompagnateur(EnhancedEnum):
    COMPLETER_LE_FORMULAIRE_DADHESION = (
        "completer-le-formulaire-dadhesion",
        "Compléter le formulaire d’adhésion",
        "Compléter le formulaire d’adhésion associé.",
    )
    ENVOYER_UN_MAIL = (
        "envoyer-un-mail",
        "Envoyer un courriel",
        "Envoyer un courriel à l’adresse indiquée.",
    )
    ENVOYER_UN_MAIL_AVEC_UNE_FICHE_DE_PRESCRIPTION = (
        "envoyer-un-mail-avec-une-fiche-de-prescription",
        "Envoyer un courriel avec une fiche de prescription",
        "Envoyer un courriel à l’adresse indiquée avec une fiche de prescription.",
    )
    TELEPHONER = (
        "telephoner",
        "Téléphoner",
        "Téléphoner au numéro indiqué.",
    )
    PRENDRE_RDV = (
        "prendre-rdv",
        "Prendre rendez-vous",
        "Prendre rendez-vous auprès de la structure",
    )
    AUTRE = (
        "autre",
        "Autre",
        None,
    )


class ModeOrientationBeneficiaire(EnhancedEnum):
    COMPLETER_LE_FORMULAIRE_DADHESION = (
        "completer-le-formulaire-dadhesion",
        "Compléter le formulaire d’adhésion",
        "Compléter le formulaire d’adhésion associé.",
    )
    ENVOYER_UN_MAIL = (
        "envoyer-un-mail",
        "Envoyer un courriel",
        "Envoyer un courriel à l’adresse indiquée.",
    )
    SE_PRESENTER = (
        "se-presenter",
        "Se présenter",
        "Se présenter à l’adresse indiquée.",
    )
    TELEPHONER = (
        "telephoner",
        "Téléphoner",
        "Téléphoner au numéro indiqué.",
    )
    PRENDRE_RDV = (
        "prendre-rdv",
        "Prendre rendez-vous",
        "Prendre rendez-vous auprès de la structure",
    )
    AUTRE = (
        "autre",
        "Autre",
        None,
    )
