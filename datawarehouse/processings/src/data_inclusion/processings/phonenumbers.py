from typing import Optional

import phonenumbers


def format_phone_number(phone_number: str) -> Optional[str]:
    try:
        parsed_number = phonenumbers.parse(phone_number, None)
    except phonenumbers.NumberParseException as e:
        if e.error_type == phonenumbers.NumberParseException.INVALID_COUNTRY_CODE:
            try:
                parsed_number = phonenumbers.parse(phone_number, "FR")
            except phonenumbers.NumberParseException:
                return None
        else:
            return None

    if phonenumbers.is_possible_number(parsed_number) and phonenumbers.is_valid_number(
        parsed_number
    ):
        if parsed_number.country_code == 33:
            return phonenumbers.format_number(
                parsed_number, phonenumbers.PhoneNumberFormat.NATIONAL
            )
        return phonenumbers.format_number(
            parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL
        )
