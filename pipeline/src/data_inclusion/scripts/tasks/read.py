import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from data_inclusion.scripts.tasks import constants
from data_inclusion.scripts.tasks.sources import (
    cd35,
    cd72,
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

READ_BY_SOURCE_TYPE = {
    constants.SourceType.CD35: cd35.read_data,
    constants.SourceType.CD72: cd72.read_data,
    constants.SourceType.DORA: dora.read_data,
    constants.SourceType.EMPLOIS: emplois.read_data,
    constants.SourceType.ODSPEP: odspep.read_data,
    constants.SourceType.SIAO: siao.read_data,
    constants.SourceType.SOLIGUIDE: soliguide.read_data,
    constants.SourceType.MES_AIDES: mes_aides.read_data,
    constants.SourceType.FINESS: finess.read_data,
    constants.SourceType.ETAB_PUBLICS: etab_publics.read_data,
    constants.SourceType.UN_JEUNE_UNE_SOLUTION: un_jeune_une_solution.read_data,
    constants.SourceType.MEDIATION_NUMERIQUE: mediation_numerique.read_data,
}


def read(
    path: Path,
    src_type: constants.SourceType,
) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    return READ_BY_SOURCE_TYPE[src_type](path=path)
