{# !!! THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY. !!! #}

{% macro udf__is_french_mobile_phone_number() %}

DROP FUNCTION IF EXISTS processings.is_french_mobile_phone_number;

CREATE OR REPLACE FUNCTION processings.is_french_mobile_phone_number(phone TEXT)
RETURNS BOOLEAN
AS $$

import phonenumbers

FRENCH_PHONE_REGIONS = {
    "BL",
    "FR",
    "GF",
    "GP",
    "MF",
    "MQ",
    "NC",
    "PF",
    "PM",
    "RE",
    "WF",
    "YT",
}
MOBILE_PHONE_TYPES = {
    phonenumbers.PhoneNumberType.FIXED_LINE_OR_MOBILE,
    phonenumbers.PhoneNumberType.MOBILE,
}


def is_french_mobile_phone_number(value: str | None) -> bool:
    if value is None:
        return False

    try:
        parsed = phonenumbers.parse(value, region="FR")
    except phonenumbers.NumberParseException:
        return False

    return (
        phonenumbers.is_valid_number(parsed)
        and phonenumbers.region_code_for_number(parsed) in FRENCH_PHONE_REGIONS
        and phonenumbers.number_type(parsed) in MOBILE_PHONE_TYPES
    )


return is_french_mobile_phone_number(phone)

$$ LANGUAGE plpython3u;

{% endmacro %}
