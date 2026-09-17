import phonenumbers


def is_french_mobile_phone_number(value: str | None) -> bool:
    if value is None:
        return False

    try:
        parsed = phonenumbers.parse(value, region="FR")
    except phonenumbers.NumberParseException:
        return False

    return (
        phonenumbers.is_valid_number(parsed)
        and phonenumbers.region_code_for_number(parsed) == "FR"
        and phonenumbers.number_type(parsed) == phonenumbers.PhoneNumberType.MOBILE
    )
