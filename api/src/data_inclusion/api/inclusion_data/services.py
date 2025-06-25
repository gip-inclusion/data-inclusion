import abc
import functools
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Literal

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import func, or_, orm

import fastapi

# TODO(vmttn): handle pagination outside ?
from fastapi_pagination.ext.sqlalchemy import paginate

from data_inclusion.api.decoupage_administratif.constants import (
    Departement,
    Region,
)
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v0 import models as v0_models
from data_inclusion.api.inclusion_data.v1 import models as v1_models
from data_inclusion.schema import v0, v1

logger = logging.getLogger(__name__)

CODE_INSEE_FRANCE = "99100"


class ServiceLayer[
    Thematique: v0.Thematique | v1.Thematique,
    Frais: v0.Frais | v1.Frais,
    Profil: v0.Profil | v1.Public,
    TypeService: v0.TypologieService | v1.TypeService,
    TypologieStructure: v0.TypologieStructure | v1.TypologieStructure,
    LabelNational: v0.LabelNational | v1.LabelNational,
    ModeAccueil: v0.ModeAccueil | v1.ModeAccueil,
    CodeCommune: v0.CodeCommune | v1.CodeCommune,
](abc.ABC):
    """Service layer for managing structures and services.

    This class provides methods to list, retrieve, and filter structures and services.

    This class must not be used directly.

    Instead, subclass it like this:
        - set the `schema` attribute to the appropriate schema registry
        - type the generic parameters with the appropriate schema types
        - override implementations if needed (to change a signature)

    The `schema` attribute is used internally to access actual schema values.
    It can also be used to do things depending on the schema in use.
    For instance using `if self.schema is v0`.

    The generic parameters are used to type the methods of this class, and provide
    type safety in consuming code and in methods implementations.
    """

    schema_version: Literal["v0"] | Literal["v1"]

    def __init__(self) -> None:
        self.schema = v0 if self.schema_version == "v0" else v1
        self.models = v0_models if self.schema_version == "v0" else v1_models

    @functools.cache
    def get_thematiques_by_group(self) -> dict[str, list[str]]:
        thematiques = defaultdict(list)
        for thematique in self.schema.Thematique:
            try:
                theme, _ = str(thematique.value).split("--")
            except ValueError:
                continue
            thematiques[theme].append(thematique.value)
        return thematiques

    def filter_restricted(
        self,
        query: sqla.Select,
        request: fastapi.Request,
    ) -> sqla.Select:
        if not request.user.is_authenticated or not request.user.username.startswith(
            "dora-"
        ):
            query = query.filter(
                sqla.or_(
                    self.models.Structure.source != "soliguide",
                    self.models.Structure.code_insee.startswith("59"),  # Nord
                    self.models.Structure.code_insee.startswith("67"),  # Bas-Rhin
                )
            )

        return query

    def list_structures(
        self,
        request: fastapi.Request,
        db_session: orm.Session,
        sources: list[str] | None = None,
        typologie: TypologieStructure | None = None,
        label_national: LabelNational | None = None,
        departement: Departement | None = None,
        region: Region | None = None,
        commune_code: CodeCommune | None = None,
        thematiques: list[Thematique] | None = None,
        deduplicate: bool | None = False,
    ) -> list:
        query = sqla.select(self.models.Structure).options(
            orm.selectinload(self.models.Structure.doublons)
        )
        query = self.filter_restricted(query, request)

        if sources is not None:
            query = query.filter(
                self.models.Structure.source == sqla.any_(sqla.literal(sources))
            )

        if commune_code is not None:
            query = query.filter_by(code_insee=commune_code)

        if departement is not None:
            query = query.filter(
                self.models.Structure.code_insee.startswith(departement.code)
            )

        if typologie is not None:
            query = query.filter_by(typologie=typologie.value)

        if region is not None:
            query = query.join(Commune).options(
                orm.contains_eager(self.models.Structure.commune_)
            )
            query = query.filter(Commune.region == region.code)

        if label_national is not None:
            query = query.filter(
                self.models.Structure.labels_nationaux.contains([label_national.value])
            )

        if thematiques is not None:
            query = query.filter(
                sqla.text(
                    f"{self.models.Structure.__tablename__}.thematiques && :thematiques"
                ).bindparams(
                    thematiques=self.get_sub_thematiques(thematiques=thematiques),
                )
            )

        if deduplicate:
            query = query.filter(
                sqla.or_(
                    self.models.Structure._cluster_id.is_(None),
                    self.models.Structure._is_best_duplicate.is_(True),
                )
            )

        query = query.order_by(self.models.Structure._di_surrogate_id)

        return paginate(db_session, query)

    def retrieve_structure(
        self,
        db_session: orm.Session,
        source: str,
        id_: str,
    ):
        query = (
            sqla.select(self.models.Structure)
            .options(orm.selectinload(self.models.Structure.services))
            .options(orm.selectinload(self.models.Structure.doublons))
            .filter_by(source=source)
            .filter_by(id=id_)
        )

        structure_instance = db_session.scalars(query).first()

        if structure_instance is None:
            raise fastapi.HTTPException(status_code=404)

        return structure_instance

    @functools.cache
    def list_sources(self, request: fastapi.Request) -> list[dict]:
        return json.loads((Path(__file__).parent / "sources.json").read_text())

    def get_sub_thematiques(self, thematiques: list[Thematique]) -> list[str]:
        all_thematiques = set()
        for t in thematiques:
            all_thematiques.add(t.value)
            group = self.get_thematiques_by_group()[t.value]
            if group:
                all_thematiques.update(group)
        return list(all_thematiques)

    def _filter_array_field(self, query, model, field_name, values):
        filter_stmt = f"""\
        EXISTS(
            SELECT
            FROM unnest({model.__tablename__}.{field_name}) {field_name}
            WHERE {field_name} = ANY(:{field_name})
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(**{field_name: [f.value for f in values]})
        )
        return query

    def _filter_zone(
        self, commune_instance: Commune, query: sqla.Select
    ) -> sqla.Select:
        return query.filter(
            sqla.or_(
                self.models.Service.zone_diffusion_type.is_(None),
                self.models.Service.zone_diffusion_type
                == self.schema.ZoneDiffusionType.PAYS.value,
                sqla.and_(
                    self.models.Service.zone_diffusion_type
                    == self.schema.ZoneDiffusionType.COMMUNE.value,
                    self.models.Service.zone_diffusion_code == commune_instance.code,
                ),
                sqla.and_(
                    self.models.Service.zone_diffusion_type
                    == self.schema.ZoneDiffusionType.EPCI.value,
                    sqla.literal(commune_instance.siren_epci).contains(
                        self.models.Service.zone_diffusion_code
                    ),
                ),
                sqla.and_(
                    self.models.Service.zone_diffusion_type
                    == self.schema.ZoneDiffusionType.DEPARTEMENT.value,
                    self.models.Service.zone_diffusion_code
                    == commune_instance.departement,
                ),
                sqla.and_(
                    self.models.Service.zone_diffusion_type
                    == self.schema.ZoneDiffusionType.REGION.value,
                    self.models.Service.zone_diffusion_code == commune_instance.region,
                ),
            )
        )

    def filter_services(
        self,
        query: sqla.Select,
        sources: list[str] | None = None,
        thematiques: list[Thematique] | None = None,
        frais: list[Frais] | None = None,
        profils: list[Profil] | None = None,
        profils_search: str | None = None,
        modes_accueil: list[ModeAccueil] | None = None,
        types: list[TypeService] | None = None,
        score_qualite_minimum: float | None = None,
    ) -> sqla.Select:
        if sources is not None:
            query = query.filter(
                self.models.Service.source == sqla.any_(sqla.literal(sources))
            )

        if thematiques is not None:
            query = query.filter(
                sqla.text(
                    f"{self.models.Service.__tablename__}.thematiques && :thematiques"
                ).bindparams(
                    thematiques=self.get_sub_thematiques(thematiques=thematiques),
                )
            )

        if frais is not None:
            query = self._filter_array_field(query, self.models.Service, "frais", frais)

        if profils is not None:
            query = self._filter_array_field(
                query, self.models.Service, "profils", profils
            )

        if modes_accueil is not None:
            query = self._filter_array_field(
                query,
                self.models.Service,
                "modes_accueil",
                modes_accueil,
            )

        if types is not None:
            query = self._filter_array_field(query, self.models.Service, "types", types)

        if score_qualite_minimum is not None:
            query = query.filter(
                self.models.Service.score_qualite >= score_qualite_minimum
            )

        if profils_search is not None:
            profils_only = profils_search.split(" ")
            profils_only = [p.strip() for p in profils_only]
            query = query.filter(
                or_(
                    self.models.Service.searchable_index_profils.bool_op("@@")(
                        func.to_tsquery("french_di", " | ".join(profils_only))
                    ),
                    self.models.Service.searchable_index_profils_precisions.bool_op(
                        "@@"
                    )(func.websearch_to_tsquery("french_di", profils_search)),
                )
            )

        return query

    def list_services(
        self,
        request: fastapi.Request,
        db_session: orm.Session,
        sources: list[str] | None = None,
        thematiques: list[Thematique] | None = None,
        departement: Departement | None = None,
        region: Region | None = None,
        code_commune: CodeCommune | None = None,
        frais: list[Frais] | None = None,
        profils: list[Profil] | None = None,
        recherche_public: str | None = None,
        modes_accueil: list[ModeAccueil] | None = None,
        types: list[TypeService] | None = None,
        score_qualite_minimum: float | None = None,
    ):
        query = (
            sqla.select(self.models.Service)
            .join(self.models.Structure)
            .options(orm.contains_eager(self.models.Service.structure))
        )
        query = self.filter_restricted(query, request)

        if departement is not None:
            query = query.filter(
                self.models.Service.code_insee.startswith(departement.code)
            )

        if region is not None:
            query = query.join(Commune).options(
                orm.contains_eager(self.models.Service.commune_)
            )
            query = query.filter(Commune.region == region.code)

        if code_commune is not None:
            query = query.filter(self.models.Service.code_insee == code_commune)

        query = self.filter_services(
            query=query,
            sources=sources,
            thematiques=thematiques,
            frais=frais,
            profils=profils,
            profils_search=recherche_public,
            modes_accueil=modes_accueil,
            types=types,
            score_qualite_minimum=score_qualite_minimum,
        )

        query = query.order_by(self.models.Service._di_surrogate_id)

        return paginate(db_session, query, unique=False)

    def search_services(
        self,
        request: fastapi.Request,
        db_session: orm.Session,
        sources: list[str] | None = None,
        commune_instance: Commune | None = None,
        thematiques: list[Thematique] | None = None,
        frais: list[Frais] | None = None,
        modes_accueil: list[ModeAccueil] | None = None,
        profils: list[Profil] | None = None,
        profils_search: str | None = None,
        types: list[TypeService] | None = None,
        search_point: str | None = None,
        score_qualite_minimum: float | None = None,
        deduplicate: bool | None = False,
    ):
        query = (
            sqla.select(self.models.Service)
            .join(self.models.Structure)
            .options(orm.contains_eager(self.models.Service.structure))
        )
        query = self.filter_restricted(query, request)

        if commune_instance is not None:
            query = self._filter_zone(commune_instance, query)

            src_geometry = sqla.cast(
                geoalchemy2.functions.ST_MakePoint(
                    self.models.Service.longitude, self.models.Service.latitude
                ),
                geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
            )

            if search_point is not None:
                dest_geometry = search_point
            else:
                dest_geometry = commune_instance.centre

            query = query.filter(
                sqla.or_(
                    # either `en-presentiel` within a given distance
                    geoalchemy2.functions.ST_DWithin(
                        src_geometry,
                        dest_geometry,
                        50_000,  # meters
                    ),
                    # or `a-distance`
                    self.models.Service.modes_accueil.contains(
                        sqla.literal([self.schema.ModeAccueil.A_DISTANCE.value])
                    ),
                )
            )

            # annotate distance
            query = query.add_columns(
                (
                    sqla.case(
                        (
                            self.models.Service.modes_accueil.contains(
                                sqla.literal(
                                    [self.schema.ModeAccueil.EN_PRESENTIEL.value]
                                )
                            ),
                            (
                                geoalchemy2.functions.ST_Distance(
                                    src_geometry,
                                    dest_geometry,
                                )
                                / 1000  # conversion to kms
                            ).cast(sqla.Integer),
                        ),
                        else_=sqla.null().cast(sqla.Integer),
                    )
                ).label("distance")
            )

        else:
            query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

        query = self.filter_services(
            query=query,
            sources=sources,
            thematiques=thematiques,
            frais=frais,
            profils=profils,
            profils_search=profils_search,
            modes_accueil=modes_accueil,
            types=types,
            score_qualite_minimum=score_qualite_minimum,
        )

        if deduplicate:
            query = query.filter(
                sqla.or_(
                    self.models.Structure._cluster_id.is_(None),
                    self.models.Structure._is_best_duplicate.is_(True),
                )
            )

        query = query.order_by(
            sqla.column("distance").nulls_last(),
            self.models.Service._di_surrogate_id,
        )

        def _items_to_mappings(items: list) -> list[dict]:
            # convert rows returned by `Session.execute` to a list of dicts that will be
            # used to instanciate pydantic models
            return [{"service": item[0], "distance": item[1]} for item in items]

        return paginate(db_session, query, unique=False, transformer=_items_to_mappings)

    def retrieve_service(
        self,
        db_session: orm.Session,
        source: str,
        id_: str,
    ):
        query = (
            sqla.select(self.models.Service)
            .join(self.models.Structure)
            .filter(self.models.Service.source == source)
            .filter(self.models.Service.id == id_)
        )

        service_instance = db_session.scalars(query).first()

        if service_instance is None:
            raise fastapi.HTTPException(status_code=404)

        return service_instance


class ServiceLayerV0(
    ServiceLayer[
        v0.Thematique,
        v0.Frais,
        v0.Profil,
        v0.TypologieService,
        v0.TypologieStructure,
        v0.LabelNational,
        v0.ModeAccueil,
        v0.CodeCommune,
    ]
):
    schema_version = "v0"


class ServiceLayerV1(
    ServiceLayer[
        v1.Thematique,
        v1.Frais,
        v1.Public,
        v1.TypeService,
        v1.TypologieStructure,
        v1.LabelNational,
        v1.ModeAccueil,
        v1.CodeCommune,
    ]
):
    schema_version = "v1"

    # NOTE(vperron) : I decided to override the `filter_services` method entirely
    # as it introduces way less "magic" than having N mini methods, one for every field
    # that gets overriden.
    def filter_services(
        self,
        query: sqla.Select,
        sources: list[str] | None = None,
        thematiques: list[v1.Thematique] | None = None,
        frais: list[v1.Frais] | None = None,
        profils: list[v1.Public] | None = None,
        profils_search: str | None = None,
        modes_accueil: list[v1.ModeAccueil] | None = None,
        types: list[v1.TypeService] | None = None,
        score_qualite_minimum: float | None = None,
    ) -> sqla.Select:
        if sources is not None:
            query = query.filter(
                self.models.Service.source == sqla.any_(sqla.literal(sources))
            )

        if thematiques is not None:
            query = query.filter(
                sqla.text(
                    f"{self.models.Service.__tablename__}.thematiques && :thematiques"
                ).bindparams(
                    thematiques=self.get_sub_thematiques(thematiques=thematiques),
                )
            )

        if frais is not None:
            query = query.filter(
                self.models.Service.frais == sqla.any_(sqla.literal(frais))
            )

        if profils is not None:
            query = self._filter_array_field(
                query, self.models.Service, "publics", profils
            )

        if modes_accueil is not None:
            query = self._filter_array_field(
                query,
                self.models.Service,
                "modes_accueil",
                modes_accueil,
            )

        if types is not None:
            query = query.filter(
                self.models.Service.type == sqla.any_(sqla.literal(types))
            )

        if score_qualite_minimum is not None:
            query = query.filter(
                self.models.Service.score_qualite >= score_qualite_minimum
            )

        if profils_search is not None:
            publics_only = profils_search.split(" ")
            publics_only = [p.strip() for p in publics_only]
            query = query.filter(
                or_(
                    self.models.Service.searchable_index_publics.bool_op("@@")(
                        func.to_tsquery("french_di", " | ".join(publics_only))
                    ),
                    self.models.Service.searchable_index_publics_precisions.bool_op(
                        "@@"
                    )(func.websearch_to_tsquery("french_di", profils_search)),
                )
            )

        return query

    def _filter_zone(
        self, commune_instance: Commune, query: sqla.Select
    ) -> sqla.Select:
        return query.filter(
            sqla.or_(
                self.models.Service.zone_eligibilite.is_(None),
                self.models.Service.zone_eligibilite.op("&&")(
                    sqla.literal(
                        [
                            commune_instance.code,
                            commune_instance.departement,
                            commune_instance.siren_epci,
                            CODE_INSEE_FRANCE,
                        ]
                    )
                ),
            )
        )

    def list_structures(
        self,
        request: fastapi.Request,
        db_session: orm.Session,
        sources: list[str] | None = None,
        typologie: v1.TypologieStructure | None = None,
        label_national: v1.LabelNational | None = None,
        departement: Departement | None = None,
        region: Region | None = None,
        commune_code: v1.CodeCommune | None = None,
        deduplicate: bool | None = False,
    ) -> list:
        # structures does not have thematiques in v1
        return super().list_structures(
            request=request,
            db_session=db_session,
            sources=sources,
            typologie=typologie,
            label_national=label_national,
            departement=departement,
            region=region,
            commune_code=commune_code,
            thematiques=None,  # reuse the base method, but ignore thematiques
            deduplicate=deduplicate,
        )
