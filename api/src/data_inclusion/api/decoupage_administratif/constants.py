"""This module contains constants related to the official geographical codes.

cf https://www.insee.fr/fr/information/2560452
"""

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class Departement:
    slug: str
    code: str


class DepartementEnum(Enum):
    AIN = Departement("ain", "01")
    AISNE = Departement("aisne", "02")
    ALLIER = Departement("allier", "03")
    ALPES_DE_HAUTE_PROVENCE = Departement("alpes-de-haute-provence", "04")
    ALPES_MARITIMES = Departement("alpes-maritimes", "06")
    ARDECHE = Departement("ardeche", "07")
    ARDENNES = Departement("ardennes", "08")
    ARIEGE = Departement("ariege", "09")
    AUBE = Departement("aube", "10")
    AUDE = Departement("aude", "11")
    AVEYRON = Departement("aveyron", "12")
    BAS_RHIN = Departement("bas-rhin", "67")
    BOUCHES_DU_RHONE = Departement("bouches-du-rhone", "13")
    CALVADOS = Departement("calvados", "14")
    CANTAL = Departement("cantal", "15")
    CHARENTE_MARITIME = Departement("charente-maritime", "17")
    CHARENTE = Departement("charente", "16")
    CHER = Departement("cher", "18")
    CORREZE = Departement("correze", "19")
    CORSE_DU_SUD = Departement("corse-du-sud", "2A")
    COTE_D_OR = Departement("cote-d-or", "21")
    COTES_D_ARMOR = Departement("cotes-d-armor", "22")
    CREUSE = Departement("creuse", "23")
    DEUX_SEVRES = Departement("deux-sevres", "79")
    DORDOGNE = Departement("dordogne", "24")
    DOUBS = Departement("doubs", "25")
    DROME = Departement("drome", "26")
    ESSONNE = Departement("essonne", "91")
    EURE_ET_LOIR = Departement("eure-et-loir", "28")
    EURE = Departement("eure", "27")
    FINISTERE = Departement("finistere", "29")
    GARD = Departement("gard", "30")
    GERS = Departement("gers", "32")
    GIRONDE = Departement("gironde", "33")
    GUADELOUPE = Departement("guadeloupe", "971")
    GUYANE = Departement("guyane", "973")
    HAUT_RHIN = Departement("haut-rhin", "68")
    HAUTE_CORSE = Departement("haute-corse", "2B")
    HAUTE_GARONNE = Departement("haute-garonne", "31")
    HAUTE_LOIRE = Departement("haute-loire", "43")
    HAUTE_MARNE = Departement("haute-marne", "52")
    HAUTE_SAONE = Departement("haute-saone", "70")
    HAUTE_SAVOIE = Departement("haute-savoie", "74")
    HAUTE_VIENNE = Departement("haute-vienne", "87")
    HAUTES_ALPES = Departement("hautes-alpes", "05")
    HAUTES_PYRENEES = Departement("hautes-pyrenees", "65")
    HAUTS_DE_SEINE = Departement("hauts-de-seine", "92")
    HERAULT = Departement("herault", "34")
    ILLE_ET_VILAINE = Departement("ille-et-vilaine", "35")
    INDRE_ET_LOIRE = Departement("indre-et-loire", "37")
    INDRE = Departement("indre", "36")
    ISERE = Departement("isere", "38")
    JURA = Departement("jura", "39")
    LA_REUNION = Departement("la-reunion", "974")
    LANDES = Departement("landes", "40")
    LOIR_ET_CHER = Departement("loir-et-cher", "41")
    LOIRE_ATLANTIQUE = Departement("loire-atlantique", "44")
    LOIRE = Departement("loire", "42")
    LOIRET = Departement("loiret", "45")
    LOT_ET_GARONNE = Departement("lot-et-garonne", "47")
    LOT = Departement("lot", "46")
    LOZERE = Departement("lozere", "48")
    MAINE_ET_LOIRE = Departement("maine-et-loire", "49")
    MANCHE = Departement("manche", "50")
    MARNE = Departement("marne", "51")
    MARTINIQUE = Departement("martinique", "972")
    MAYENNE = Departement("mayenne", "53")
    MAYOTTE = Departement("mayotte", "976")
    MEURTHE_ET_MOSELLE = Departement("meurthe-et-moselle", "54")
    MEUSE = Departement("meuse", "55")
    MORBIHAN = Departement("morbihan", "56")
    MOSELLE = Departement("moselle", "57")
    NIEVRE = Departement("nievre", "58")
    NORD = Departement("nord", "59")
    OISE = Departement("oise", "60")
    ORNE = Departement("orne", "61")
    PARIS = Departement("paris", "75")
    PAS_DE_CALAIS = Departement("pas-de-calais", "62")
    PUY_DE_DOME = Departement("puy-de-dome", "63")
    PYRENEES_ATLANTIQUES = Departement("pyrenees-atlantiques", "64")
    PYRENEES_ORIENTALES = Departement("pyrenees-orientales", "66")
    RHONE = Departement("rhone", "69")
    SAONE_ET_LOIRE = Departement("saone-et-loire", "71")
    SARTHE = Departement("sarthe", "72")
    SAVOIE = Departement("savoie", "73")
    SEINE_ET_MARNE = Departement("seine-et-marne", "77")
    SEINE_MARITIME = Departement("seine-maritime", "76")
    SEINE_SAINT_DENIS = Departement("seine-saint-denis", "93")
    SOMME = Departement("somme", "80")
    TARN_ET_GARONNE = Departement("tarn-et-garonne", "82")
    TARN = Departement("tarn", "81")
    TERRITOIRE_DE_BELFORT = Departement("territoire-de-belfort", "90")
    VAL_D_OISE = Departement("val-d-oise", "95")
    VAL_DE_MARNE = Departement("val-de-marne", "94")
    VAR = Departement("var", "83")
    VAUCLUSE = Departement("vaucluse", "84")
    VENDEE = Departement("vendee", "85")
    VIENNE = Departement("vienne", "86")
    VOSGES = Departement("vosges", "88")
    YONNE = Departement("yonne", "89")
    YVELINES = Departement("yvelines", "78")


DepartementSlugEnum = Enum(
    "DepartementSlugEnum",
    {member.name: member.value.slug for member in DepartementEnum},
)
DepartementCodeEnum = Enum(
    "DepartementCodeEnum",
    {member.name: member.value.code for member in DepartementEnum},
)


@dataclass(frozen=True)
class Region:
    slug: str
    code: str


class RegionEnum(Enum):
    AUVERGNE_RHONE_ALPES = Region("auvergne-rhone-alpes", "84")
    BOURGOGNE_FRANCHE_COMTE = Region("bourgogne-franche-comte", "27")
    BRETAGNE = Region("bretagne", "53")
    CENTRE_VAL_DE_LOIRE = Region("centre-val-de-loire", "24")
    CORSE = Region("corse", "94")
    GRAND_EST = Region("grand-est", "44")
    GUADELOUPE = Region("guadeloupe", "01")
    GUYANE = Region("guyane", "03")
    HAUTS_DE_FRANCE = Region("hauts-de-france", "32")
    ILE_DE_FRANCE = Region("ile-de-france", "11")
    LA_REUNION = Region("la-reunion", "04")
    MARTINIQUE = Region("martinique", "02")
    MAYOTTE = Region("mayotte", "06")
    NORMANDIE = Region("normandie", "28")
    NOUVELLE_AQUITAINE = Region("nouvelle-aquitaine", "75")
    OCCITANIE = Region("occitanie", "76")
    PAYS_DE_LA_LOIRE = Region("pays-de-la-loire", "52")
    PROVENCE_ALPES_COTE_D_AZUR = Region("provence-alpes-cote-d-azur", "93")


RegionSlugEnum = Enum(
    "RegionSlugEnum",
    {member.name: member.value.slug for member in RegionEnum},
)
RegionCodeEnum = Enum(
    "RegionCodeEnum",
    {member.name: member.value.code for member in RegionEnum},
)
