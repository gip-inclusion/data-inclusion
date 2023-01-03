import logging

from sirene import models

logger = logging.getLogger(__name__)


def get_naf_data_by_code(level: models.CodeNAF.Level, code: str):
    level_str = level.value if level != models.CodeNAF.Level.SUBCLASS else ""

    data = models.CodeNAF.objects.filter(**{f"{level_str}_code": code}).values(f"{level_str}_label").distinct().first()

    if data is None:
        logger.warn(f"No match for code {code} at level {level}")
        return None

    return {"code": code, "level": level, "label": data[f"{level_str}_label"]}
