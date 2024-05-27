from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class _Departement:
    slug: str
    cog: str


_departements_dict = {
    "AIN": _Departement("ain", "01"),
    "AISNE": _Departement("aisne", "02"),
    "ALLIER": _Departement("allier", "03"),
    "ALPES_DE_HAUTE_PROVENCE": _Departement("alpes-de-haute-provence", "04"),
    "HAUTES_ALPES": _Departement("hautes-alpes", "05"),
    "ALPES_MARITIMES": _Departement("alpes-maritimes", "06"),
    "ARDECHE": _Departement("ardeche", "07"),
    "ARDENNES": _Departement("ardennes", "08"),
    "ARIEGE": _Departement("ariege", "09"),
    "AUBE": _Departement("aube", "10"),
    "AUDE": _Departement("aude", "11"),
    "AVEYRON": _Departement("aveyron", "12"),
    "BOUCHES_DU_RHONE": _Departement("bouches-du-rhone", "13"),
    "CALVADOS": _Departement("calvados", "14"),
    "CANTAL": _Departement("cantal", "15"),
    "CHARENTE": _Departement("charente", "16"),
    "CHARENTE_MARITIME": _Departement("charente-maritime", "17"),
    "CHER": _Departement("cher", "18"),
    "CORREZE": _Departement("correze", "19"),
    "COTE_D_OR": _Departement("cote-d-or", "21"),
    "COTES_D_ARMOR": _Departement("cotes-d-armor", "22"),
    "CREUSE": _Departement("creuse", "23"),
    "DORDOGNE": _Departement("dordogne", "24"),
    "DOUBS": _Departement("doubs", "25"),
    "DROME": _Departement("drome", "26"),
    "EURE": _Departement("eure", "27"),
    "EURE_ET_LOIR": _Departement("eure-et-loir", "28"),
    "FINISTERE": _Departement("finistere", "29"),
    "CORSE_DU_SUD": _Departement("corse-du-sud", "2A"),
    "HAUTE_CORSE": _Departement("haute-corse", "2B"),
    "GARD": _Departement("gard", "30"),
    "HAUTE_GARONNE": _Departement("haute-garonne", "31"),
    "GERS": _Departement("gers", "32"),
    "GIRONDE": _Departement("gironde", "33"),
    "HERAULT": _Departement("herault", "34"),
    "ILLE_ET_VILAINE": _Departement("ille-et-vilaine", "35"),
    "INDRE": _Departement("indre", "36"),
    "INDRE_ET_LOIRE": _Departement("indre-et-loire", "37"),
    "ISERE": _Departement("isere", "38"),
    "JURA": _Departement("jura", "39"),
    "LANDES": _Departement("landes", "40"),
    "LOIR_ET_CHER": _Departement("loir-et-cher", "41"),
    "LOIRE": _Departement("loire", "42"),
    "HAUTE_LOIRE": _Departement("haute-loire", "43"),
    "LOIRE_ATLANTIQUE": _Departement("loire-atlantique", "44"),
    "LOIRET": _Departement("loiret", "45"),
    "LOT": _Departement("lot", "46"),
    "LOT_ET_GARONNE": _Departement("lot-et-garonne", "47"),
    "LOZERE": _Departement("lozere", "48"),
    "MAINE_ET_LOIRE": _Departement("maine-et-loire", "49"),
    "MANCHE": _Departement("manche", "50"),
    "MARNE": _Departement("marne", "51"),
    "HAUTE_MARNE": _Departement("haute-marne", "52"),
    "MAYENNE": _Departement("mayenne", "53"),
    "MEURTHE_ET_MOSELLE": _Departement("meurthe-et-moselle", "54"),
    "MEUSE": _Departement("meuse", "55"),
    "MORBIHAN": _Departement("morbihan", "56"),
    "MOSELLE": _Departement("moselle", "57"),
    "NIEVRE": _Departement("nievre", "58"),
    "NORD": _Departement("nord", "59"),
    "OISE": _Departement("oise", "60"),
    "ORNE": _Departement("orne", "61"),
    "PAS_DE_CALAIS": _Departement("pas-de-calais", "62"),
    "PUY_DE_DOME": _Departement("puy-de-dome", "63"),
    "PYRENEES_ATLANTIQUES": _Departement("pyrenees-atlantiques", "64"),
    "HAUTES_PYRENEES": _Departement("hautes-pyrenees", "65"),
    "PYRENEES_ORIENTALES": _Departement("pyrenees-orientales", "66"),
    "BAS_RHIN": _Departement("bas-rhin", "67"),
    "HAUT_RHIN": _Departement("haut-rhin", "68"),
    "RHONE": _Departement("rhone", "69"),
    "HAUTE_SAONE": _Departement("haute-saone", "70"),
    "SAONE_ET_LOIRE": _Departement("saone-et-loire", "71"),
    "SARTHE": _Departement("sarthe", "72"),
    "SAVOIE": _Departement("savoie", "73"),
    "HAUTE_SAVOIE": _Departement("haute-savoie", "74"),
    "PARIS": _Departement("paris", "75"),
    "SEINE_MARITIME": _Departement("seine-maritime", "76"),
    "SEINE_ET_MARNE": _Departement("seine-et-marne", "77"),
    "YVELINES": _Departement("yvelines", "78"),
    "DEUX_SEVRES": _Departement("deux-sevres", "79"),
    "SOMME": _Departement("somme", "80"),
    "TARN": _Departement("tarn", "81"),
    "TARN_ET_GARONNE": _Departement("tarn-et-garonne", "82"),
    "VAR": _Departement("var", "83"),
    "VAUCLUSE": _Departement("vaucluse", "84"),
    "VENDEE": _Departement("vendee", "85"),
    "VIENNE": _Departement("vienne", "86"),
    "HAUTE_VIENNE": _Departement("haute-vienne", "87"),
    "VOSGES": _Departement("vosges", "88"),
    "YONNE": _Departement("yonne", "89"),
    "TERRITOIRE_DE_BELFORT": _Departement("territoire-de-belfort", "90"),
    "ESSONNE": _Departement("essonne", "91"),
    "HAUTS_DE_SEINE": _Departement("hauts-de-seine", "92"),
    "SEINE_SAINT_DENIS": _Departement("seine-saint-denis", "93"),
    "VAL_DE_MARNE": _Departement("val-de-marne", "94"),
    "VAL_D_OISE": _Departement("val-d-oise", "95"),
    "GUADELOUPE": _Departement("guadeloupe", "971"),
    "MARTINIQUE": _Departement("martinique", "972"),
    "GUYANE": _Departement("guyane", "973"),
    "LA_REUNION": _Departement("la-reunion", "974"),
    "MAYOTTE": _Departement("mayotte", "976"),
}


DepartementSlug = Enum(
    "DepartementSlug",
    {k: departement.slug for k, departement in _departements_dict.items()},
)
DepartementCOG = Enum(
    "DepartementCOG",
    {k: departement.cog for k, departement in _departements_dict.items()},
)


@dataclass(frozen=True)
class _Region:
    slug: str
    cog: str


_regions_dict = {
    "GUADELOUPE": _Region("guadeloupe", "01"),
    "MARTINIQUE": _Region("martinique", "02"),
    "GUYANE": _Region("guyane", "03"),
    "LA_REUNION": _Region("la-reunion", "04"),
    "MAYOTTE": _Region("mayotte", "06"),
    "ILE_DE_FRANCE": _Region("ile-de-france", "11"),
    "CENTRE_VAL_DE_LOIRE": _Region("centre-val-de-loire", "24"),
    "BOURGOGNE_FRANCHE_COMTE": _Region("bourgogne-franche-comte", "27"),
    "NORMANDIE": _Region("normandie", "28"),
    "HAUTS_DE_FRANCE": _Region("hauts-de-france", "32"),
    "GRAND_EST": _Region("grand-est", "44"),
    "PAYS_DE_LA_LOIRE": _Region("pays-de-la-loire", "52"),
    "BRETAGNE": _Region("bretagne", "53"),
    "NOUVELLE_AQUITAINE": _Region("nouvelle-aquitaine", "75"),
    "OCCITANIE": _Region("occitanie", "76"),
    "AUVERGNE_RHONE_ALPES": _Region("auvergne-rhone-alpes", "84"),
    "PROVENCE_ALPES_COTE_D_AZUR": _Region("provence-alpes-cote-d-azur", "93"),
    "CORSE": _Region("corse", "94"),
}

RegionSlug = Enum(
    "RegionSlug",
    {k: region.slug for k, region in _regions_dict.items()},
)
RegionCOG = Enum(
    "RegionCOG",
    {k: region.cog for k, region in _regions_dict.items()},
)


# based on
# https://github.com/gip-inclusion/dora-back/blob/main/dora/admin_express/utils.py

_DISTRICTS_BY_CITY = {
    # Paris
    "75056": [
        "75101",
        "75102",
        "75103",
        "75104",
        "75105",
        "75106",
        "75107",
        "75108",
        "75109",
        "75110",
        "75111",
        "75112",
        "75113",
        "75114",
        "75115",
        "75116",
        "75117",
        "75118",
        "75119",
        "75120",
    ],
    # Lyon
    "69123": [
        "69381",
        "69382",
        "69383",
        "69384",
        "69385",
        "69386",
        "69387",
        "69388",
        "69389",
    ],
    # Marseille
    "13055": [
        "13201",
        "13202",
        "13203",
        "13204",
        "13205",
        "13206",
        "13207",
        "13208",
        "13209",
        "13210",
        "13211",
        "13212",
        "13213",
        "13214",
        "13215",
        "13216",
    ],
}

CODE_COMMUNE_BY_CODE_ARRONDISSEMENT = {
    code_arrondissement: code_commune
    for code_commune, codes_arrondissements in _DISTRICTS_BY_CITY.items()
    for code_arrondissement in codes_arrondissements
}
