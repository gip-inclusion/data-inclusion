from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional, TypeAlias

from pydantic import BaseModel, EmailStr, HttpUrl, constr


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


class Typologie(str, Enum):
    ACI = "ACI"
    ACIPHC = "ACIPHC"
    AFPA = "AFPA"
    AI = "AI"
    ASE = "ASE"
    ASSO = "ASSO"
    ASSO_CHOMEUR = "ASSO_CHOMEUR"
    Autre = "Autre"
    BIB = "BIB"
    CAARUD = "CAARUD"
    CADA = "CADA"
    CAF = "CAF"
    CAP_EMPLOI = "CAP_EMPLOI"
    CAVA = "CAVA"
    CC = "CC"
    CCAS = "CCAS"
    CCONS = "CCONS"
    CD = "CD"
    CHRS = "CHRS"
    CHU = "CHU"
    CIAS = "CIAS"
    CIDFF = "CIDFF"
    CITMET = "CITMET"
    CPH = "CPH"
    CS = "CS"
    CSAPA = "CSAPA"
    DEETS = "DEETS"
    DEPT = "DEPT"
    DIPLP = "DIPLP"
    E2C = "E2C"
    EA = "EA"
    EATT = "EATT"
    EI = "EI"
    EITI = "EITI"
    EPCI = "EPCI"
    EPIDE = "EPIDE"
    ETTI = "ETTI"
    FAIS = "FAIS"
    GEIQ = "GEIQ"
    HUDA = "HUDA"
    MDE = "MDE"
    MDEF = "MDEF"
    MDPH = "MDPH"
    MDS = "MDS"
    MJC = "MJC"
    ML = "ML"
    MQ = "MQ"
    MSA = "MSA"
    MUNI = "MUNI"
    OACAS = "OACAS"
    ODC = "ODC"
    OF = "OF"
    OIL = "OIL"
    OPCS = "OPCS"
    PAD = "PAD"
    PE = "PE"
    PENSION = "PENSION"
    PIJ_BIJ = "PIJ_BIJ"
    PIMMS = "PIMMS"
    PJJ = "PJJ"
    PLIE = "PLIE"
    PREF = "PREF"
    PREVENTION = "PREVENTION"
    REG = "REG"
    RFS = "RFS"
    RS_FJT = "RS_FJT"
    SCP = "SCP"
    SPIP = "SPIP"
    TIERS_LIEUX = "TIERS_LIEUX"
    UDAF = "UDAF"


class LabelNational(str, Enum):
    ACTION_LOGEMENT = "ACTION_LOGEMENT"
    ADIE = "ADIE"
    AFPA = "AFPA"
    AGEFIPH = "AGEFIPH"
    AIDANTS_CONNECT = "AIDANTS_CONNECT"
    ALLIANCE_VILLES_EMPLOI = "ALLIANCE_VILLES_EMPLOI"
    ANLCI = "ANLCI"
    APEC = "APEC"
    APELS = "APELS"
    APPRENTIS_DAUTEUIL = "APPRENTIS_DAUTEUIL"
    APTIC = "APTIC"
    BANQUES_ALIMENTAIRES = "BANQUES_ALIMENTAIRES"
    BGE = "BGE"
    CAF = "CAF"
    CAMPUS_CONNECTE = "CAMPUS_CONNECTE"
    CCI = "CCI"
    CHANTIER_ECOLE = "CHANTIER_ECOLE"
    CHEOPS = "CHEOPS"
    CIDFF = "CIDFF"
    CNAM = "CNAM"
    COORACE = "COORACE"
    CREPI = "CREPI"
    CRESUS = "CRESUS"
    CROIX_ROUGE = "CROIX_ROUGE"
    DIHAL = "DIHAL"
    DUO_FOR_A_JOB = "DUO_FOR_A_JOB"
    ECOLES_DE_LA_DEUXIEME_CHANCE = "ECOLES_DE_LA_DEUXIEME_CHANCE"
    EGEE = "EGEE"
    EMMAUS = "EMMAUS"
    ENVIE = "ENVIE"
    EPIDE = "EPIDE"
    ESPACE_EMPLOI_AGRIC_ARRCO = "ESPACE_EMPLOI_AGRIC_ARRCO"
    FABRIQUE_DE_TERRITOIRE = "FABRIQUE_DE_TERRITOIRE"
    FACE = "FACE"
    FEDE_PRO_FEM = "FEDE_PRO_FEM"
    FEDERATION_DES_ACTEURS_DE_LA_SOLIDARITE = "FEDERATION_DES_ACTEURS_DE_LA_SOLIDARITE"
    FEDERATION_DES_ENTREPRISES_DINSERTION = "FEDERATION_DES_ENTREPRISES_DINSERTION"
    FORCE_FEMMES = "FORCE_FEMMES"
    FRANCE_ACTIVE = "FRANCE_ACTIVE"
    FRANCE_SERVICE = "FRANCE_SERVICE"
    FRENCH_TECH = "FRENCH_TECH"
    GEIQ = "GEIQ"
    GRAND_TEST_COUVEUSE_DENTREPRISE = "GRAND_TEST_COUVEUSE_DENTREPRISE"
    GRANDES_ECOLES_DU_NUMERIQUE = "GRANDES_ECOLES_DU_NUMERIQUE"
    HUP = "HUP"
    INAE = "INAE"
    INITIATIVE_FRANCE = "INITIATIVE_FRANCE"
    KONEXIO = "KONEXIO"
    LA_CRAVATE_SOLIDAIRE = "LA_CRAVATE_SOLIDAIRE"
    LA_RESSOURCERIE = "LA_RESSOURCERIE"
    LES_PREMIERES = "LES_PREMIERES"
    LINKLUSION = "LINKLUSION"
    MAISONS_DE_LEMPLOI = "MAISONS_DE_LEMPLOI"
    MDPH = "MDPH"
    MISSION_LOCALE = "MISSION_LOCALE"
    MOBIN = "MOBIN"
    NQT = "NQT"
    POINT_CONSEIL_BUDGET = "POINT_CONSEIL_BUDGET"
    POINT_JUSTICE = "POINT_JUSTICE"
    POLE_EMPLOI = "POLE_EMPLOI"
    POSITIVE_PLANET = "POSITIVE_PLANET"
    PROXITE = "PROXITE"
    RESEAU_ENTREPRENDRE = "RESEAU_ENTREPRENDRE"
    RESSOURCERIES = "RESSOURCERIES"
    RESTOS_DU_COEUR = "RESTOS_DU_COEUR"
    SCCONSEIL = "SCCONSEIL"
    SECOURS_POPULAIRE = "SECOURS_POPULAIRE"
    SIAO = "SIAO"
    SIMPLON = "SIMPLON"
    SINCA = "SINCA"
    SNC = "SNC"
    SOIXANTE_MILLE_REBONDS = "60000_REBONDS"
    SPORT_DANS_LA_VILLE = "SPORT_DANS_LA_VILLE"
    TOUS_TES_POSSIBLE = "TOUS_TES_POSSIBLE"
    TZCLD = "TZCLD"
    UNAF = "UNAF"
    UNAI = "UNAI"
    UNCCAS = "UNCCAS"
    UNEA = "UNEA"
    UNIS_CITE = "UNIS_CITE"


class Thematique(str, Enum):
    CHOISIR_UN_METIER = "choisir-un-metier"
    CHOISIR_UN_METIER_IDENTIFIER_SES_POINTS_FORTS_ET_SES_COMPETENCES = (
        "choisir-un-metier-identifier-ses-points-forts-et-ses-competences"
    )
    CHOISIR_UN_METIER_CONNAITRE_LES_OPPORTUNITES_DEMPLOI = (
        "choisir-un-metier-connaitre-les-opportunites-demploi"
    )
    CHOISIR_UN_METIER_DECOUVRIR_UN_METIER_OU_UN_SECTEUR_DACTIVITE = (
        "choisir-un-metier-decouvrir-un-metier-ou-un-secteur-dactivite"
    )
    CHOISIR_UN_METIER_CONFIRMER_SON_CHOIX_DE_METIER = (
        "choisir-un-metier-confirmer-son-choix-de-metier"
    )

    CREATION_ACTIVITE = "creation-activite"
    CREATION_ACTIVITE_DE_L_IDEE_AU_PROJET = "creation-activite-de-l-idee-au-projet"
    CREATION_ACTIVITE_DEMARRER_SON_ACTIVITE = "creation-activite-demarrer-son-activite"
    CREATION_ACTIVITE_ELABORER_SON_PROJET = "creation-activite-elaborer-son-projet"
    CREATION_ACTIVITE_RESEAUTAGE_POUR_CREATEURS_DENTREPRISE = (
        "creation-activite-reseautage-pour-createurs-dentreprise"
    )
    CREATION_ACTIVITE_FINANCER_SON_PROJET = "creation-activite-financer-son-projet"

    MOBILITE = "mobilite"
    MOBILITE_ETRE_ACCOMPAGNE_DANS_SON_PARCOURS_MOBILITE = (
        "mobilite-etre-accompagne-dans-son-parcours-mobilite"
    )
    MOBILITE_ENTRETENIR_REPARER_SON_VEHICULE = (
        "mobilite-entretenir-reparer-son-vehicule"
    )
    MOBILITE_LOUER_UN_VEHICULE = "mobilite-louer-un-vehicule"
    MOBILITE_ACHETER_UN_VEHICULE_MOTORISE = "mobilite-acheter-un-vehicule-motorise"
    MOBILITE_PREPARER_SON_PERMIS_DE_CONDUIRE_SE_REENTRAINER_A_LA_CONDUITE = (
        "mobilite-preparer-son-permis-de-conduire-se-reentrainer-a-la-conduite"
    )
    MOBILITE_AIDES_A_LA_REPRISE_DEMPLOI_OU_A_LA_FORMATION = (
        "mobilite-aides-a-la-reprise-demploi-ou-a-la-formation"
    )
    MOBILITE_COMPRENDRE_ET_UTILISER_LES_TRANSPORTS_EN_COMMUN = (
        "mobilite-comprendre-et-utiliser-les-transports-en-commun"
    )
    MOBILITE_APPRENDRE_A_UTILISER_UN_DEUX_ROUES = (
        "mobilite-apprendre-a-utiliser-un-deux-roues"
    )
    MOBILITE_FINANCER_MON_PROJET_MOBILITE = "mobilite-financer-mon-projet-mobilite"
    MOBILITE_ACHETER_UN_VELO = "mobilite-acheter-un-velo"

    NUMERIQUE = "numerique"
    NUMERIQUE_DEVENIR_AUTONOME_DANS_LES_DEMARCHES_ADMINISTRATIVES = (
        "numerique-devenir-autonome-dans-les-demarches-administratives"
    )
    NUMERIQUE_REALISER_DES_DEMARCHES_ADMINISTRATIVES_AVEC_UN_ACCOMPAGNEMENT = (
        "numerique-realiser-des-demarches-administratives-avec-un-accompagnement"
    )
    NUMERIQUE_PRENDRE_EN_MAIN_UN_SMARTPHONE_OU_UNE_TABLETTE = (
        "numerique-prendre-en-main-un-smartphone-ou-une-tablette"
    )
    NUMERIQUE_PRENDRE_EN_MAIN_UN_ORDINATEUR = "numerique-prendre-en-main-un-ordinateur"
    NUMERIQUE_UTILISER_LE_NUMERIQUE_AU_QUOTIDIEN = (
        "numerique-utiliser-le-numerique-au-quotidien"
    )
    NUMERIQUE_APPROFONDIR_MA_CULTURE_NUMERIQUE = (
        "numerique-approfondir-ma-culture-numerique"
    )
    NUMERIQUE_FAVORISER_MON_INSERTION_PROFESSIONNELLE = (
        "numerique-favoriser-mon-insertion-professionnelle"
    )
    NUMERIQUE_ACCEDER_A_UNE_CONNEXION_INTERNET = (
        "numerique-acceder-a-une-connexion-internet"
    )
    NUMERIQUE_ACCEDER_A_DU_MATERIEL = "numerique-acceder-a-du-materiel"
    NUMERIQUE_S_EQUIPER_EN_MATERIEL_INFORMATIQUE = (
        "numerique-s-equiper-en-materiel-informatique"
    )
    NUMERIQUE_CREER_ET_DEVELOPPER_MON_ENTREPRIES = (
        "numerique-creer-et-developper-mon-entrepries"
    )
    NUMERIQUE_CREER_AVEC_LE_NUMERIQUE = "numerique-creer-avec-le-numerique"
    NUMERIQUE_ACCOMPAGNER_LES_DEMARCHES_DE_SANTE = (
        "numerique-accompagner-les-demarches-de-sante"
    )
    NUMERIQUE_PROMOUVOIR_LA_CITOYENNETE_NUMERIQUE = (
        "numerique-promouvoir-la-citoyennete-numerique"
    )
    NUMERIQUE_SOUTENIR_LA_PARENTALITE_ET_L_EDUCATION_AVEC_LE_NUMERIQUE = (
        "numerique-soutenir-la-parentalite-et-l-education-avec-le-numerique"
    )

    PREPARER_SA_CANDIDATURE = "preparer-sa-candidature"
    PREPARER_SA_CANDIDATURE_VALORISER_SES_COMPETENCES = (
        "preparer-sa-candidature-valoriser-ses-competences"
    )
    PREPARER_SA_CANDIDATURE_REALISER_UN_CV_ET_OU_UNE_LETTRE_DE_MOTIVATION = (
        "preparer-sa-candidature-realiser-un-cv-et-ou-une-lettre-de-motivation"
    )
    PREPARER_SA_CANDIDATURE_DEVELOPPER_SON_RESEAU = (
        "preparer-sa-candidature-developper-son-reseau"
    )
    PREPARER_SA_CANDIDATURE_ORGANISER_SES_DEMARCHES_DE_RECHERCHE_DEMPLOI = (
        "preparer-sa-candidature-organiser-ses-demarches-de-recherche-demploi"
    )

    TROUVER_UN_EMPLOI = "trouver-un-emploi"
    TROUVER_UN_EMPLOI_REPONDRE_A_DES_OFFRES_DEMPLOI = (
        "trouver-un-emploi-repondre-a-des-offres-demploi"
    )
    TROUVER_UN_EMPLOI_FAIRE_DES_CANDIDATURES_SPONTANEES = (
        "trouver-un-emploi-faire-des-candidatures-spontanees"
    )
    TROUVER_UN_EMPLOI_SUIVRE_SES_CANDIDATURES_ET_RELANCER_LES_EMPLOYEURS = (
        "trouver-un-emploi-suivre-ses-candidatures-et-relancer-les-employeurs"
    )
    TROUVER_UN_EMPLOI_CONVAINCRE_UN_RECRUTEUR_EN_ENTRETIEN = (
        "trouver-un-emploi-convaincre-un-recruteur-en-entretien"
    )


class TypologieService(str, Enum):
    ACCOMPAGNEMENT = "accompagnement"
    ACCUEIL = "accueil"
    AIDE_FINANCIERE = "aide-financiere"
    AIDE_MATERIELLE = "aide-materielle"
    ATELIER = "atelier"
    FORMATION = "formation"
    INFORMATION = "information"
    NUMÉRIQUE = "numerique"
    AUTONOMIE = "autonomie"
    DELEGATION = "delegation"
    FINANCEMENT = "financement"


class Frais(str, Enum):
    GRATUIT = "gratuit"
    GRATUIT_SOUS_CONDITIONS = "gratuit-sous-conditions"
    PAYANT = "payant"
    ADHESION = "adhesion"


class Service(BaseModel):
    id: str
    structure_id: str
    source: str
    nom: str
    presentation_resume: Optional[constr(max_length=280)]
    types: Optional[list[TypologieService]]
    thematiques: Optional[list[Thematique]]
    prise_rdv: Optional[HttpUrl]
    frais: Optional[list[Frais]]
    frais_autres: Optional[str]

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
    site_web: Optional[HttpUrl]
    presentation_resume: Optional[constr(max_length=280)]
    presentation_detail: Optional[str]
    source: str
    date_maj: date | datetime
    structure_parente: Optional[str]
    lien_source: Optional[HttpUrl]
    horaires_ouverture: Optional[str]
    accessibilite: Optional[HttpUrl]
    labels_nationaux: Optional[list[LabelNational]]
    labels_autres: Optional[list[str]]
    thematiques: Optional[list[Thematique]]

    class Config:
        orm_mode = True
