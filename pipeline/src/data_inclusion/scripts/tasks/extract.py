import io
import logging
from typing import Optional

from data_inclusion.scripts.tasks import constants
from data_inclusion.scripts.tasks.sources import (
    cd35,
    cd72,
    conseiller_numerique,
    dora,
    emplois,
    etab_publics,
    finess,
    mediation_numerique,
    mes_aides,
    odspep,
    siao,
    soliguide,
    un_jeune_une_solution,
)

logger = logging.getLogger(__name__)

EXTRACT_BY_SOURCE_TYPE = {
    constants.SourceType.CD35: cd35.extract_data,
    constants.SourceType.CD72: cd72.extract_data,
    constants.SourceType.DORA: dora.extract_data,
    constants.SourceType.EMPLOIS: emplois.extract_data,
    constants.SourceType.ODSPEP: odspep.extract_data,
    constants.SourceType.SIAO: siao.extract_data,
    constants.SourceType.SOLIGUIDE: soliguide.extract_data,
    constants.SourceType.MES_AIDES: mes_aides.extract_data,
    constants.SourceType.FINESS: finess.extract_data,
    constants.SourceType.ETAB_PUBLICS: etab_publics.extract_data,
    constants.SourceType.UN_JEUNE_UNE_SOLUTION: un_jeune_une_solution.extract_data,
    constants.SourceType.CONSEILLER_NUMERIQUE: conseiller_numerique.extract_data,
    constants.SourceType.MEDIATION_NUMERIQUE: mediation_numerique.extract_data,
}


def extract(
    src: str,
    src_type: constants.SourceType,
    token: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> dict[str, io.BytesIO]:
    logger.info("[EXTRACTION]")

    extract_fn = EXTRACT_BY_SOURCE_TYPE.get(src_type, None)

    if extract_fn is None:
        return {}

    return extract_fn(src=src, token=token, user_agent=user_agent)
