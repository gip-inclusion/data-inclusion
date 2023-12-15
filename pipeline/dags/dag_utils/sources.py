import functools
import json

from airflow.models import Variable

from data_inclusion.scripts.tasks import (
    agefiph,
    annuaire_du_service_public,
    dora,
    emplois_de_linclusion,
    mediation_numerique,
    mes_aides,
    reseau_alpha,
    soliguide,
    un_jeune_une_solution,
    utils,
)


class UrlMixin:
    url_var = None

    @property
    def url(self):
        return Variable.get(self.url_var) if self.url_var else None


class StreamConfig(UrlMixin):
    def __init__(self, filename, url_var=None, extract_fn=None, load_fn=None):
        self.filename = filename
        self.url_var = url_var
        self.extract_fn = extract_fn or self._default_extract
        self.load_fn = load_fn or utils.read_json

    def _default_extract(self):
        return utils.extract_http_content(self.url)

    @functools.cached_property
    def id(self):
        return self.filename.split(".")[0]


class SourceConfig(UrlMixin):
    id = ""
    schedule = "@daily"
    should_snapshot = True
    streams = []

    @functools.cached_property
    def stream_map(self):
        return {s.id: s for s in self.streams}

    def stream(self, stream_id):
        return self.stream_map[stream_id]

    def load(self):
        # implement here anything that requires lazy loading
        pass


class DoraSourceConfig(SourceConfig):
    id = "dora"
    url_var = "DORA_API_URL"
    streams = [StreamConfig("structures.json"), StreamConfig("services.json")]

    def load(self) -> None:
        client = dora.DoraClient(self.url, Variable.get("DORA_API_TOKEN"))
        self.stream("structures").extract_fn = client.list_structures
        self.stream("services").extract_fn = client.list_services


class MediationNumeriqueSourceConfig(SourceConfig):
    id = "mediation-numerique"
    url_var = "MEDNUM_API_URL"
    streams = [StreamConfig("structures.json"), StreamConfig("services.json")]

    def load(self) -> None:
        def extract_structures():
            return mediation_numerique.extract("structures", self.url)

        def extract_services():
            return mediation_numerique.extract("services", self.url)

        self.stream("structures").extract_fn = extract_structures
        self.stream("services").extract_fn = extract_services


class MesAidesSourceConfig(SourceConfig):
    id = "mes-aides"
    streams = [
        StreamConfig("garages.json", "MES_AIDES_GARAGES_URL"),
        StreamConfig("aides.json", "MES_AIDES_AIDES_URL"),
    ]

    def load(self) -> None:
        token = Variable.get("MES_AIDES_AIRTABLE_KEY")
        garages = self.stream("garages")
        garages.extract_fn = lambda: mes_aides.extract(garages.url, token)
        aides = self.stream("aides")
        aides.extract_fn = lambda: mes_aides.extract(aides.url, token)


class FinessSourceConfig(SourceConfig):
    id = "finess"
    streams = [
        StreamConfig(
            filename="etablissements.xlsx",
            url_var="FINESS_FILE_URL",
            load_fn=lambda path: utils.read_csv(path, sep=","),
        )
    ]


class Cd35SourceConfig(SourceConfig):
    id = "cd35"
    streams = [
        StreamConfig(
            filename="organisations.csv",
            url_var="CD35_FILE_URL",
            load_fn=lambda path: utils.read_csv(path, sep=";"),
        )
    ]


class LesEmploisSourceConfig(SourceConfig):
    id = "emplois-de-linclusion"
    url_var = "EMPLOIS_API_URL"
    streams = [
        StreamConfig(
            filename="siaes.json",
        ),
        StreamConfig(
            filename="organisations.json",
        ),
    ]

    def load(self) -> None:
        client = emplois_de_linclusion.EmploisClient(
            self.url, Variable.get("EMPLOIS_API_TOKEN")
        )
        self.stream("siaes").extract_fn = client.list_siaes
        self.stream("organisations").extract_fn = client.list_organisations


class AnnuaireDuServicePublicSourceConfig(SourceConfig):
    id = "annuaire-du-service-public"
    streams = [
        StreamConfig(
            filename="etablissements.json",
            url_var="ETAB_PUB_FILE_URL",
            load_fn=annuaire_du_service_public.read,
        ),
    ]


class UnJeuneUneSolutionSourceConfig(SourceConfig):
    id = "un-jeune-une-solution"
    url_var = "UN_JEUNE_UNE_SOLUTION_API_URL"
    streams = [
        StreamConfig(filename="benefits.json"),
        StreamConfig(filename="institutions.json"),
    ]

    def load(self) -> None:
        def extract_benefits():
            return un_jeune_une_solution.extract("benefits", self.url)

        def extract_institutions():
            return un_jeune_une_solution.extract("institutions", self.url)

        self.stream("benefits").extract_fn = extract_benefits
        self.stream("institutions").extract_fn = extract_institutions


class SoliguideSourceConfig(SourceConfig):
    id = "soliguide"
    url_var = "SOLIGUIDE_API_URL"
    streams = [StreamConfig(filename="lieux.json")]

    def load(self) -> None:
        client = soliguide.APIClient(self.url, Variable.get("SOLIGUIDE_API_TOKEN"))

        def extract_fn():
            return json.dumps(
                client.search(
                    location_geo_type="pays",
                    location_geo_value="france",
                )
            ).encode()

        self.stream("lieux").extract_fn = extract_fn


class AgefiphSourceConfig(SourceConfig):
    id = "agefiph"
    streams = [
        StreamConfig(
            filename="services.json",
            url_var="AGEFIPH_SERVICES_API_URL",
            load_fn=agefiph.read,
        ),
    ]


class MonEnfantSourceConfig(SourceConfig):
    id = "monenfant"
    schedule = "@once"
    streams = [
        StreamConfig(
            filename="creches.json",
            url_var="MONENFANT_CRECHES_FILE_URL",
        ),
    ]


class ReseauAlphaSourceConfig(SourceConfig):
    id = "reseau-alpha"
    schedule = "@once"
    should_snapshot = False
    url_var = "RESEAU_ALPHA_URL"
    streams = [
        StreamConfig(filename="structures.tar.gz"),
        StreamConfig(filename="formations.tar.gz"),
    ]

    def load(self) -> None:
        self.stream("structures").extract_fn = lambda: reseau_alpha.extract_structures(
            self.url
        )
        self.stream("structures").read_fn = reseau_alpha.read_structures
        self.stream("formations").extract_fn = lambda: reseau_alpha.extract_formations(
            self.url
        )
        self.stream("formations").read_fn = reseau_alpha.read_formations


class DataInclusionSourceConfig(SourceConfig):
    # NB: the name "data-inclusion" is displayed in Metabase and Dora maps.
    # It is ambiguous but easier to understand to our customers.
    id = "data-inclusion"
    schedule = "@once"
    should_snapshot = False
    streams = [
        StreamConfig(
            filename="services.csv",
            url_var="DI_EXTRA_SERVICES_FILE_URL",
            load_fn=lambda path: utils.read_csv(path, sep=","),
        ),
        StreamConfig(
            filename="structures.csv",
            url_var="DI_EXTRA_STRUCTURES_FILE_URL",
            load_fn=lambda path: utils.read_csv(path, sep=","),
        ),
    ]


#
# class Cd72SourceConfig(SourceConfig):
#     id = "cd72"
#     schedule = "@once"
#     should_snapshot = False
#     streams = [
#         StreamConfig(
#             filename="structures.csv",
#             url=Variable.get("CD72_STRUCTURES_FILE_URL"),
#             load_fn=lambda path: utils.read_csv(path, sep=","),
#         ),
#         StreamConfig(
#             filename="services.csv",
#             url=Variable.get("CD72_SERVICES_FILE_URL"),
#             load_fn=lambda path: utils.read_csv(path, sep=","),
#         ),
#     ]
#
#     def __init__(self) -> None:
#         structures_url = Variable.get("CD72_STRUCTURES_FILE_URL")
#         services_url = Variable.get("CD72_SERVICES_FILE_URL")
#         grist_api_token = Variable.get("GRIST_API_TOKEN")
#
#         def extract_structures():
#             grist.extract(structures_url, grist_api_token)
#
#         def extract_services():
#             grist.extract(services_url, grist_api_token)
#
#         self.streams = [
#             StreamConfig(
#                 filename="structures.csv",
#                 url=Variable.get("CD72_STRUCTURES_FILE_URL"),
#                 extract_fn=extract_structures,
#                 load_fn=lambda path: utils.read_csv(path, sep=","),
#             ),
#             StreamConfig(
#                 filename="services.csv",
#                 url=Variable.get("CD72_SERVICES_FILE_URL"),
#                 extract_fn=extract_services,
#                 load_fn=lambda path: utils.read_csv(path, sep=","),
#             ),
#         ]
#
#
# class PoleEmploiSourceConfig(SourceConfig):
#     id = "pole-emploi"
#     schedule = "@once"
#     should_snapshot = False
#
#     def __init__(self) -> None:
#         url = Variable.get("DORA_PREPROD_API_URL", None)
#         client = dora.DoraClient(url, Variable.get("DORA_PREPROD_API_TOKEN", None))
#         self.streams = [
#             StreamConfig(
#                 filename="structures.json", url=url, extract_fn=client.list_structures
#             ),
#             StreamConfig(
#                 filename="services.json", url=url, extract_fn=client.list_services
#             ),
#         ]
#
#
# class SiaoSourceConfig(SourceConfig):
#     id = "siao"
#     schedule = "@once"
#     should_snapshot = False
#     streams = [
#         StreamConfig(
#             filename="etablissements.xlsx",
#             url=Variable.get("SIAO_FILE_URL"),
#             load_fn=utils.read_excel,
#         )
#     ]


SOURCES_CONFIGS = [
    DoraSourceConfig,
    MediationNumeriqueSourceConfig,
    MesAidesSourceConfig,
    LesEmploisSourceConfig,
    AnnuaireDuServicePublicSourceConfig,
    UnJeuneUneSolutionSourceConfig,
    # sources under investigation
    AgefiphSourceConfig,
    Cd35SourceConfig,
    # Cd72SourceConfig,
    FinessSourceConfig,
    SoliguideSourceConfig,
    # one-shot sources
    DataInclusionSourceConfig,
    MonEnfantSourceConfig,
    # PoleEmploiSourceConfig,
    ReseauAlphaSourceConfig,
    # SiaoSourceConfig,
]

SOURCES_MAP = {s.id: s for s in SOURCES_CONFIGS}


def load(source_id):
    source = SOURCES_MAP[source_id]()
    source.load()
    return source
