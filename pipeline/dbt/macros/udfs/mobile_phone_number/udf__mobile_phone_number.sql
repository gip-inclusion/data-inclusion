{# !!! THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY. !!! #}

{% macro udf__is_french_mobile_phone_number() %}

DROP FUNCTION IF EXISTS processings.is_french_mobile_phone_number;

CREATE OR REPLACE FUNCTION processings.is_french_mobile_phone_number(phone TEXT)
RETURNS BOOLEAN
AS $$

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


return is_french_mobile_phone_number(phone)

$$ LANGUAGE plpython3u;

{% endmacro %}
