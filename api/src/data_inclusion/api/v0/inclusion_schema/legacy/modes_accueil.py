from data_inclusion.schema.base import EnhancedEnum


class ModeAccueil(EnhancedEnum):
    A_DISTANCE = (
        "a-distance",
        "À distance",
        "Le service est proposé à distance, par téléphone ou internet.",
    )
    EN_PRESENTIEL = (
        "en-presentiel",
        "En présentiel",
        "Le service est proposé en présentiel, à l'adresse associée à ce service.",
    )
