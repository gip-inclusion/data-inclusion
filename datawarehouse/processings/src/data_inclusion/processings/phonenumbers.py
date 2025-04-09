import re
from typing import Optional

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

# https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
ALLOWED_REGION_CODES = [
    "YT",  # Mayotte
    "MQ",  # Martinique
    "FR",  # France
    "RE",  # Réunion
    "GF",  # Guyane
    "GP",  # Guadeloupe
]

FOUR_DIGIT_PATTERN = re.compile(r"^3\d{3}$")


def format_phone_number(phone_number: str) -> Optional[str]:
    if phone_number is None:
        return None

    phone_number = phone_number.replace(" ", "")

    if phone_number in URGENCY_NUMBERS or FOUR_DIGIT_PATTERN.fullmatch(phone_number):
        return phone_number

    for region_code in ALLOWED_REGION_CODES:
        try:
            parsed_number = phonenumbers.parse(phone_number, region_code)
        except phonenumbers.NumberParseException:
            continue

        if phonenumbers.is_possible_number(
            parsed_number
        ) and phonenumbers.is_valid_number(parsed_number):
            return phonenumbers.format_number(
                parsed_number,
                phonenumbers.PhoneNumberFormat.E164,
            )

    return None
