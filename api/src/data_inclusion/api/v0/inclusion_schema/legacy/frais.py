from data_inclusion.schema.base import EnhancedEnum


class Frais(EnhancedEnum):
    GRATUIT = (
        "gratuit",
        "Gratuit",
        "Je peux accéder gratuitement au lieu et à ses services",
    )
    GRATUIT_SOUS_CONDITIONS = (
        "gratuit-sous-conditions",
        "Gratuit sous conditions",
        """
            La gratuité est conditionnée à des critères (situation familiale,
            convention avec un organisme social…)
        """,
    )
    PAYANT = (
        "payant",
        "Payant",
        "L’accès au lieu et/ou à ses services est payant",
    )
    ADHESION = (
        "adhesion",
        "Adhésion",
        "L’accès au lieu et/ou à ses services nécessite d’y adhérer",
    )
    PASS_NUMERIQUE = (
        "pass-numerique",
        "Accepte le pass numérique",
        "Il est possible d’utiliser un Pass numérique pour accéder au lieu",
    )
