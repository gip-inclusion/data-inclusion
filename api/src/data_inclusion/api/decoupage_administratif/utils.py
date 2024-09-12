from data_inclusion.api.decoupage_administratif import constants


def get_departement_by_code_or_slug(
    code: constants.DepartementCodeEnum | None = None,
    slug: constants.DepartementSlugEnum | None = None,
) -> constants.Departement | None:
    if code is not None:
        return constants.DepartementEnum[code.name].value
    if slug is not None:
        return constants.DepartementEnum[slug.name].value


def get_region_by_code_or_slug(
    code: constants.RegionCodeEnum | None = None,
    slug: constants.RegionSlugEnum | None = None,
) -> constants.Region | None:
    if code is not None:
        return constants.RegionEnum[code.name].value
    if slug is not None:
        return constants.RegionEnum[slug.name].value
