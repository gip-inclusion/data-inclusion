import enum

import phonenumbers
from phonenumbers import shortnumberinfo

# note: DOMs have their own country codes, but it is also possible to use the +33 code
# because they are departements. We will favour local country codes in our outputs, but
# both local and national codes should be considered as valid inputs.

# note: TOMs are not departements. Therefore the +33 code is not valid for them.
# Nevertheless we will try and parse numbers with +33 that are obviously for a TOM.

# resources:
# - https://fr.wikipedia.org/wiki/Indicatif_téléphonique_local_en_France
# - https://fr.wikipedia.org/wiki/Liste_des_préfixes_des_opérateurs_de_téléphonie_mobile_en_France


class Region(enum.Enum):
    # see https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2

    INTERNATIONAL = None
    FRANCE = "FR"
    MAYOTTE = "YT"
    GUADELOUPE = "GP"
    GUYANE = "GF"
    MARTINIQUE = "MQ"
    REUNION = "RE"
    SAINT_PIERRE_ET_MIQUELON = "PM"
    WALLIS_ET_FUTUNA = "WF"
    POLYNESIE_FRANCAISE = "PF"
    NOUVELLE_CALEDONIE = "NC"


def format_phone_number(value: str | None) -> str | None:
    if value is None:
        return None

    # is_possible_number -> check shape and length, cheap
    # is_valid_number -> check ranges, expensive

    # short numbers (including emergency numbers) in France
    try:
        parsed = phonenumbers.parse(value, region="FR")
        if shortnumberinfo.is_possible_short_number(parsed):
            if shortnumberinfo.is_valid_short_number(parsed):
                return phonenumbers.normalize_digits_only(value)
    except phonenumbers.NumberParseException:
        pass

    # regular numbers
    # order matters: DOM TOM first, then France, then international
    for region in [
        Region.MAYOTTE,
        Region.GUADELOUPE,
        Region.GUYANE,
        Region.MARTINIQUE,
        Region.REUNION,
        Region.SAINT_PIERRE_ET_MIQUELON,
        Region.WALLIS_ET_FUTUNA,
        Region.POLYNESIE_FRANCAISE,
        Region.NOUVELLE_CALEDONIE,
        Region.FRANCE,
        Region.INTERNATIONAL,
    ]:
        try:
            parsed = phonenumbers.parse(
                value,
                region=region.value,
                keep_raw_input=True,  # required for .country_code_source
            )
        except phonenumbers.NumberParseException:
            continue

        # skip toll-free and voip numbers unless it is France
        if region != Region.FRANCE and (
            str(parsed.national_number).startswith("8")  # toll-free
            or str(parsed.national_number).startswith("9")  # voip
        ):
            continue

        # try and straighten (invalid) DOM TOM numbers using the +33
        # overwrite with the current region country code before parsing
        if (
            parsed.country_code_source
            == phonenumbers.CountryCodeSource.FROM_NUMBER_WITH_PLUS_SIGN
            and str(parsed.country_code) == "33"
            and region
            not in [
                Region.INTERNATIONAL,
                Region.FRANCE,
                Region.WALLIS_ET_FUTUNA,
                Region.POLYNESIE_FRANCAISE,
                Region.NOUVELLE_CALEDONIE,
            ]
        ):
            parsed.country_code = phonenumbers.country_code_for_region(region.value)

        # skip 6 digit numbers without code: they cannot be parsed without
        # an additional hint on the region
        if (
            parsed.country_code_source
            == phonenumbers.CountryCodeSource.FROM_DEFAULT_COUNTRY
            and len(str(parsed.national_number)) == 6
        ):
            continue

        # try and straighten (invalid) Saint-Pierre-et-Miquelon numbers
        # that are missing the right country code
        # note: not possible for other DOM TOM because their prefix is not specific
        if region in [
            Region.SAINT_PIERRE_ET_MIQUELON,
        ] and str(parsed.national_number).startswith(str(parsed.country_code)):
            parsed.national_number = int(
                str(parsed.national_number)[len(str(parsed.country_code)) :]
            )

        if phonenumbers.is_possible_number(parsed):
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                )
