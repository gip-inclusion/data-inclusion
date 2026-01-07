import re

import phonenumbers

# source: https://fr.wikipedia.org/wiki/Plan_de_num%C3%A9rotation_en_France#Num%C3%A9ros_d'urgence
URGENCY_NUMBERS = [
    "15",  # SAMU
    "17",  # police ou gendarmerie
    "18",  # pompiers
    "110",  # numéro pour des opérations de collecte de dons
    "112",  # numéro d'urgence européen
    "114",  # numéro d'urgence pour personnes sourdes ou malentendantes
    "115",  # urgences sociales (ou "SAMU social")
    "119",  # enfance maltraitée
    "191",  # service d'urgences pour les accidents aériens
    "196",  # service d'urgences en mer (depuis 2015)
    "197",  # numéro d'urgence en cas d'Alerte Enlèvement ou d'Alerte Attentat
    "116000",  # enfants disparus, SOS enfants disparus
    "116117",  # permanence des soins ambulatoires
]

# Numéros courts SVA (Services à Valeur Ajoutée) à 4 chiffres commençant par 3
# https://fr.wikipedia.org/wiki/Num%C3%A9ro_de_t%C3%A9l%C3%A9phone_en_France#SVA
SVA_NUMBER_PATTERN = re.compile(r"^3\d{3}$")

# Préfixes DOM-TOM vers code région ISO 3166-1 alpha-2
# Ordre important : RE avant YT pour le préfixe 0262 partagé
DOM_TOM_PREFIXES = {
    "0590": "GP",  # Guadeloupe
    "0690": "GP",
    "0691": "GP",
    "0594": "GF",  # Guyane
    "0694": "GF",
    "0596": "MQ",  # Martinique
    "0696": "MQ",
    "0697": "MQ",
    "0262": "RE",  # Réunion (prioritaire sur Mayotte)
    "0263": "RE",
    "0692": "RE",
    "0693": "RE",
    "0269": "YT",  # Mayotte
    "0639": "YT",
    "0508": "PM",  # Saint-Pierre-et-Miquelon
    "0608": "PM",
    "0681": "WF",  # Wallis-et-Futuna
    "0689": "PF",  # Polynésie française
    "0687": "NC",  # Nouvelle-Calédonie
}

# https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
# Ordre de priorité pour le fallback
ALLOWED_REGION_CODES = ["FR", "GP", "GF", "MQ", "RE", "YT", "PM", "WF", "PF", "NC"]

# Codes pays pour les territoires où phonenumbers ne valide pas bien
COUNTRY_CODES = {
    "PM": "508",
    "WF": "681",
    "PF": "689",
    "NC": "687",
}


def _get_region_for_prefix(phone_number: str) -> str | None:
    if len(phone_number) < 4:
        return None
    return DOM_TOM_PREFIXES.get(phone_number[:4])


def format_phone_number(phone_number: str) -> str | None:
    if phone_number is None:
        return None

    phone_number = phone_number.strip()
    phone_number = re.sub(r"[\.\-\s]", "", phone_number)

    is_urgency = phone_number in URGENCY_NUMBERS
    is_sva = SVA_NUMBER_PATTERN.fullmatch(phone_number)
    if is_urgency or is_sva:
        return phone_number

    # Numéros internationaux (hors +33)
    if phone_number.startswith("+") and not phone_number.startswith("+33"):
        try:
            parsed = phonenumbers.parse(phone_number, None)
            if phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(
                parsed
            ):
                return phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                )
        except phonenumbers.NumberParseException:
            pass
        return None

    # Conversion +33 -> 0
    phone_number = re.sub(r"^\+33", "0", phone_number)

    # Détection du préfixe DOM-TOM
    detected_region = _get_region_for_prefix(phone_number)

    # Cas spécial : WF, PF, NC ne sont pas bien supportés par phonenumbers
    # On fait donc un formatting manuel
    if detected_region in COUNTRY_CODES:
        country_code = COUNTRY_CODES[detected_region]
        national_number = phone_number.lstrip("0")
        return f"+{country_code}{national_number}"

    # Ordre de test des régions : région détectée d'abord, puis les autres
    if detected_region:
        region_codes = [detected_region] + [
            r for r in ALLOWED_REGION_CODES if r != detected_region
        ]
    else:
        region_codes = ALLOWED_REGION_CODES

    for region_code in region_codes:
        try:
            parsed = phonenumbers.parse(phone_number, region_code)
        except phonenumbers.NumberParseException:
            continue

        if phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(
            parsed
        ):
            return phonenumbers.format_number(
                parsed, phonenumbers.PhoneNumberFormat.E164
            )

    return None
