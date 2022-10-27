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
    ESS = "ESS"
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
    ACTION_LOGEMENT = "action-logement"
    ADIE = "adie"
    AFPA = "afpa"
    AGEFIPH = "agefiph"
    AIDANTS_CONNECT = "aidants-connect"
    ALLIANCE_VILLES_EMPLOI = "alliance-villes-emploi"
    ANLCI = "anlci"
    APEC = "apec"
    APELS = "apels"
    APPRENTIS_DAUTEUIL = "apprentis-dauteuil"
    APTIC = "aptic"
    BANQUES_ALIMENTAIRES = "banques-alimentaires"
    BGE = "bge"
    CAF = "caf"
    CAMPUS_CONNECTE = "campus-connecte"
    CCI = "cci"
    CHANTIER_ECOLE = "chantier-ecole"
    CHEOPS = "cheops"
    CIDFF = "cidff"
    CNAM = "cnam"
    COORACE = "coorace"
    CREPI = "crepi"
    CRESUS = "cresus"
    CROIX_ROUGE = "croix-rouge"
    DIHAL = "dihal"
    DUO_FOR_A_JOB = "duo-for-a-job"
    ECOLES_DE_LA_DEUXIEME_CHANCE = "ecoles-de-la-deuxieme-chance"
    EGEE = "egee"
    EMMAUS = "emmaus"
    ENVIE = "envie"
    EPIDE = "epide"
    ESPACE_EMPLOI_AGRIC_ARRCO = "espace-emploi-agric-arrco"
    FABRIQUE_DE_TERRITOIRE = "fabrique-de-territoire"
    FACE = "face"
    FEDE_PRO_FEM = "fede-pro-fem"
    FEDERATION_DES_ACTEURS_DE_LA_SOLIDARITE = "federation-des-acteurs-de-la-solidarite"
    FEDERATION_DES_ENTREPRISES_DINSERTION = "federation-des-entreprises-dinsertion"
    FORCE_FEMMES = "force-femmes"
    FRANCE_ACTIVE = "france-active"
    FRANCE_SERVICE = "france-service"
    FRENCH_TECH = "french-tech"
    GEIQ = "geiq"
    GRANDES_ECOLES_DU_NUMERIQUE = "grandes-ecoles-du-numerique"
    GRAND_TEST_COUVEUSE_DENTREPRISE = "grand-test-couveuse-dentreprise"
    HUP = "hup"
    INAE = "inae"
    INITIATIVE_FRANCE = "initiative-france"
    KONEXIO = "konexio"
    LA_CRAVATE_SOLIDAIRE = "la-cravate-solidaire"
    LA_RESSOURCERIE = "la-ressourcerie"
    LES_PREMIERES = "les-premieres"
    LINKLUSION = "linklusion"
    MAISONS_DE_LEMPLOI = "maisons-de-lemploi"
    MDPH = "mdph"
    MISSION_LOCALE = "mission-locale"
    MOBIN = "mobin"
    NQT = "nqt"
    POINT_CONSEIL_BUDGET = "point-conseil-budget"
    POINT_JUSTICE = "point-justice"
    POLE_EMPLOI = "pole-emploi"
    POSITIVE_PLANET = "positive-planet"
    PROXITE = "proxite"
    RESEAU_ENTREPRENDRE = "reseau-entreprendre"
    RESSOURCERIES = "ressourceries"
    RESTOS_DU_COEUR = "restos-du-coeur"
    SCCONSEIL = "scconseil"
    SECOURS_POPULAIRE = "secours-populaire"
    SIAO = "siao"
    SIMPLON = "simplon"
    SINCA = "sinca"
    SNC = "snc"
    SOIXANTE_MILLE_REBONDS = "60000-rebonds"
    SPORT_DANS_LA_VILLE = "sport-dans-la-ville"
    TOUS_TES_POSSIBLE = "tous-tes-possible"
    TZCLD = "tzcld"
    UNAF = "unaf"
    UNAI = "unai"
    UNCCAS = "unccas"
    UNEA = "unea"
    UNIS_CITE = "unis-cite"


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
    NUMERIQUE_CREER_ET_DEVELOPPER_MON_ENTREPRISE = (
        "numerique-creer-et-developper-mon-entreprise"
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
    PASS_NUMERIQUE = "pass-numerique"  # nosec: B105


class Profil(str, Enum):
    SENIORS_65 = "seniors-65"
    FAMILLES_ENFANTS = "familles-enfants"
    ADULTES = "adultes"
    JEUNES_16_26 = "jeunes-16-26"
    PUBLIC_LANGUES_ETRANGERES = "public-langues-etrangeres"
    DEFICIENCE_VISUELLE = "deficience-visuelle"
    SURDITE = "surdite"
    HANDICAPS_PSYCHIQUES = "handicaps-psychiques"
    HANDICAPS_MENTAUX = "handicaps-mentaux"
    FEMMES = "femmes"
    PERSONNES_EN_SITUATION_ILLETTRISME = "personnes-en-situation-illettrisme"


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
