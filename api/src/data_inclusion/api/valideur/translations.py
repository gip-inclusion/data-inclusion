import re

# WARNING: order matters
# translations are matched in definition order
# put more specific keys first


# ruff: disable[E501]
translations: dict[str, str] = {
    "Field required": "Champ requis",
    r"value is not a valid email address: (.*)": "La valeur n'est pas un courriel valide",
    r"Input should be a valid integer(.*)": "La valeur doit être un entier valide",
    r"Input should be a valid number(.*)": "La valeur doit être un nombre valide",
    r"Input should be greater than or equal to (.*)": "La valeur doit être supérieure ou égale à {}",
    r"Input should be greater than (.*)": "La valeur doit être supérieure à {}",
    r"Input should be less than or equal to (.*)": "La valeur doit être inférieure ou égale à {}",
    r"Input should be less than (.*)": "La valeur doit être inférieure à {}",
    r"Input should be a valid string(.*)": "La valeur doit être une chaîne de caractères valide",
    r"String should have at least (.*) characters": "La chaîne doit contenir au moins {} caractères",
    r"String should have at most (.*) characters": "La chaîne doit contenir au plus {} caractères",
    r"String should match pattern (.*)": "La chaîne doit correspondre au motif {}",
    "Input should be a valid array": "La valeur doit être une liste valide",
    "Input should be a valid dictionary": "La valeur doit être un dictionnaire valide",
    r"Input should be a valid mapping, error: (.*)": "La valeur doit être un mapping valide, erreur : {}",
    "Input should be a valid list": "La valeur doit être une liste valide",
    "Input should be a valid tuple": "La valeur doit être un tuple valide",
    "Input should be a valid set": "La valeur doit être un set valide",
    r"Input should be a valid boolean(.*)": "La valeur doit être un booléen valide (true/false)",
    r"Input should be a valid date(.*)": "La valeur doit être une date valide au format AAAA-MM-JJ",
    "Datetimes provided to dates should have zero time - e.g. be exact dates": "La valeur doit être une date valide au format AAAA-MM-JJ",
    "Date should be in the past": "La date doit être dans le passé",
    "Date should be in the future": "La date doit être dans le futur",
    "Input should be in the past": "La valeur doit être dans le passé",
    "Input should be in the future": "La valeur doit être dans le futur",
    "URL input should be a string or URL": "La valeur doit être une URL valide",
    r"Input should be a valid URL, (.*)": "La valeur doit être une URL valide",
    r"URL should have at most (.*)": "La valeur doit être une URL valide",
}
# ruff: enable[E501]


def tr(msg: str) -> str:
    for pattern, translation in translations.items():
        match = re.fullmatch(pattern, msg)

        if match is None:
            continue

        return translation.format(*match.groups())
    return msg
