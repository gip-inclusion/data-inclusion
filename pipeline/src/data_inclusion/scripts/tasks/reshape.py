import logging

import pandas as pd

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
)

logger = logging.getLogger(__name__)

RESHAPE_BY_SOURCE_TYPE = {
    constants.SourceType.CD35: cd35.transform_data,
    constants.SourceType.CD72: cd72.transform_data,
    constants.SourceType.DORA: dora.transform_data,
    constants.SourceType.EMPLOIS: emplois.transform_data,
    constants.SourceType.ODSPEP: odspep.transform_data,
    constants.SourceType.SIAO: siao.transform_data,
    constants.SourceType.SOLIGUIDE: soliguide.transform_data,
    constants.SourceType.MES_AIDES: mes_aides.transform_data,
    constants.SourceType.FINESS: finess.transform_data,
    constants.SourceType.ETAB_PUBLICS: etab_publics.transform_data,
    constants.SourceType.CONSEILLER_NUMERIQUE: conseiller_numerique.transform_data,
    constants.SourceType.MEDIATION_NUMERIQUE: mediation_numerique.transform_data,
}


def reshape(df: pd.DataFrame, src_type: constants.SourceType) -> pd.DataFrame:
    logger.info("[REMODÉLISATION]")
    return RESHAPE_BY_SOURCE_TYPE[src_type](df)
