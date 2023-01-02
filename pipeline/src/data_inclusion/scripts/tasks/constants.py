import enum


class SourceType(str, enum.Enum):
    """Types of datasources.

    Different types of source (and their corresponding format) are handled.

    Extraction and transformation will vary according to the source.
    """

    # custom sources which requires ad hoc extraction/transformation.
    CD35 = "cd35"
    CD72 = "cd72"
    DORA = "dora"
    EMPLOIS = "emplois"
    SIAO = "siao"
    ODSPEP = "odspep"
    SOLIGUIDE = "soliguide"
    MES_AIDES = "mes-aides"
    FINESS = "finess"
    ETAB_PUBLICS = "etab-publics"
    UN_JEUNE_UNE_SOLUTION = "1jeune1solution"
    CONSEILLER_NUMERIQUE = "conseiller-numerique"
