from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional, TypeAlias

from pydantic import BaseModel, EmailStr, constr

from data_inclusion.schema.models import (
    Frais,
    LabelNational,
    Profil,
    Thematique,
    Typologie,
    TypologieService,
)


class EnhancedEnumMember(BaseModel):
    value: str
    label: str
    description: Optional[str]


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


class Service(BaseModel):
    id: str
    structure_id: str
    source: str
    nom: str
    presentation_resume: Optional[constr(max_length=280)]
    types: Optional[list[TypologieService]]
    thematiques: Optional[list[Thematique]]
    prise_rdv: Optional[str]
    frais: Optional[list[Frais]]
    frais_autres: Optional[str]
    profils: Optional[list[Profil]]

    class Config:
        orm_mode = True


CodePostal: TypeAlias = constr(min_length=5, max_length=5, regex=r"^\d{5}$")


class Structure(BaseModel):
    id: str
    siret: Optional[constr(min_length=14, max_length=14, regex=r"^\d{14}$")]
    rna: Optional[constr(min_length=10, max_length=10, regex=r"^W\d{9}$")]
    nom: str
    commune: str
    code_postal: CodePostal
    code_insee: Optional[constr(min_length=5, max_length=5)]
    adresse: str
    complement_adresse: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    typologie: Optional[Typologie]
    telephone: Optional[str]
    courriel: Optional[EmailStr]
    site_web: Optional[str]
    presentation_resume: Optional[constr(max_length=280)]
    presentation_detail: Optional[str]
    source: str
    date_maj: date | datetime
    antenne: Optional[bool]
    lien_source: Optional[str]
    horaires_ouverture: Optional[str]
    accessibilite: Optional[str]
    labels_nationaux: Optional[list[LabelNational]]
    labels_autres: Optional[list[str]]
    thematiques: Optional[list[Thematique]]

    class Config:
        orm_mode = True


class TokenCreationData(BaseModel):
    email: str


class Token(BaseModel):
    access: str
