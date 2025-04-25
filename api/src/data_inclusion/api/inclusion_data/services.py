import abc
import functools
import json
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import ClassVar

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
from data_inclusion.api.inclusion_data import models
from data_inclusion.api.utils.schema_utils import SchemaV0, SchemaV1

logger = logging.getLogger(__name__)


class ServiceLayer[
    Thematique: SchemaV0.Thematique | SchemaV1.Thematique,
    Frais: SchemaV0.Frais | SchemaV1.Frais,
    Profil: SchemaV0.Profil | SchemaV1.Profil,
    TypologieService: SchemaV0.TypologieService | SchemaV1.TypologieService,
    TypologieStructure: SchemaV0.TypologieStructure | SchemaV1.TypologieStructure,
    LabelNational: SchemaV0.LabelNational | SchemaV1.LabelNational,
    ModeAccueil: SchemaV0.ModeAccueil | SchemaV1.ModeAccueil,
    CodeCommune: SchemaV0.CodeCommune | SchemaV1.CodeCommune,
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
    For instance using `isinstance(self.schema, SchemaV0)`.

    The generic parameters are used to type the methods of this class, and provide
    type safety in consuming code and in methods implementations.
    """

    schema: ClassVar[SchemaV0 | SchemaV1]

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
                    models.Structure.source != "soliguide",
                    models.Structure.code_insee.startswith("59"),  # Nord
                    models.Structure.code_insee.startswith("67"),  # Bas-Rhin
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
        query = sqla.select(models.Structure)
        query = self.filter_restricted(query, request)

        if sources is not None:
            query = query.filter(
                models.Structure.source == sqla.any_(sqla.literal(sources))
            )

        if commune_code is not None:
            query = query.filter_by(code_insee=commune_code)

        if departement is not None:
            query = query.filter(
                models.Structure.code_insee.startswith(departement.code)
            )

        if typologie is not None:
            query = query.filter_by(typologie=typologie.value)

        if region is not None:
            query = query.join(Commune).options(
                orm.contains_eager(models.Structure.commune_)
            )
            query = query.filter(Commune.region == region.code)

        if label_national is not None:
            query = query.filter(
                models.Structure.labels_nationaux.contains([label_national.value])
            )

        if thematiques is not None:
            query = query.filter(
                sqla.text("api__structures.thematiques && :thematiques").bindparams(
                    thematiques=self.get_sub_thematiques(thematiques=thematiques),
                )
            )

        if deduplicate:
            query = query.filter(
                sqla.or_(
                    models.Structure.cluster_best_duplicate.is_(None),
                    models.Structure._di_surrogate_id
                    == models.Structure.cluster_best_duplicate,
                )
            )

        return paginate(db_session, query)

    def retrieve_structure(
        self,
        db_session: orm.Session,
        source: str,
        id_: str,
    ) -> models.Structure:
        structure_instance = db_session.scalars(
            sqla.select(models.Structure)
            .options(orm.selectinload(models.Structure.services))
            .filter_by(source=source)
            .filter_by(id=id_)
        ).first()

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

    def filter_services(
        self,
        query: sqla.Select,
        sources: list[str] | None = None,
        thematiques: list[Thematique] | None = None,
        frais: list[Frais] | None = None,
        profils: list[Profil] | None = None,
        profils_search: str | None = None,
        modes_accueil: list[ModeAccueil] | None = None,
        types: list[TypologieService] | None = None,
        score_qualite_minimum: float | None = None,
        include_outdated: bool | None = False,
    ) -> sqla.Select:
        """Common filters for services."""

        if sources is not None:
            query = query.filter(
                models.Service.source == sqla.any_(sqla.literal(sources))
            )

        if thematiques is not None:
            query = query.filter(
                sqla.text("api__services.thematiques && :thematiques").bindparams(
                    thematiques=self.get_sub_thematiques(thematiques=thematiques),
                )
            )

        if frais is not None:
            filter_stmt = """\
            EXISTS(
                SELECT
                FROM unnest(api__services.frais) frais
                WHERE frais = ANY(:frais)
            )
            """
            query = query.filter(
                sqla.text(filter_stmt).bindparams(frais=[f.value for f in frais])
            )

        if profils is not None:
            filter_stmt = """\
            EXISTS(
                SELECT
                FROM unnest(api__services.profils) profils
                WHERE profils = ANY(:profils)
            )
            """
            query = query.filter(
                sqla.text(filter_stmt).bindparams(profils=[p.value for p in profils])
            )

        if modes_accueil is not None:
            filter_stmt = """\
            EXISTS(
                SELECT
                FROM unnest(api__services.modes_accueil) modes_accueil
                WHERE modes_accueil = ANY(:modes_accueil)
            )
            """
            query = query.filter(
                sqla.text(filter_stmt).bindparams(
                    modes_accueil=[m.value for m in modes_accueil]
                )
            )

        if types is not None:
            filter_stmt = """\
            EXISTS(
                SELECT
                FROM unnest(api__services.types) types
                WHERE types = ANY(:types)
            )
            """
            query = query.filter(
                sqla.text(filter_stmt).bindparams(types=[t.value for t in types])
            )

        if score_qualite_minimum is not None:
            query = query.filter(models.Service.score_qualite >= score_qualite_minimum)

        if not include_outdated:
            query = query.filter(
                sqla.or_(
                    models.Service.date_suspension.is_(None),
                    models.Service.date_suspension >= date.today(),
                )
            )

        if profils_search is not None:
            profils_only = profils_search.split(" ")
            profils_only = [p.strip() for p in profils_only]
            query = query.filter(
                or_(
                    models.Service.searchable_index_profils.bool_op("@@")(
                        func.to_tsquery("french_di", " | ".join(profils_only))
                    ),
                    models.Service.searchable_index_profils_precisions.bool_op("@@")(
                        func.websearch_to_tsquery("french_di", profils_search)
                    ),
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
        types: list[TypologieService] | None = None,
        score_qualite_minimum: float | None = None,
        include_outdated: bool | None = False,
    ):
        query = (
            sqla.select(models.Service)
            .join(models.Structure)
            .options(orm.contains_eager(models.Service.structure))
        )
        query = self.filter_restricted(query, request)

        if departement is not None:
            query = query.filter(models.Service.code_insee.startswith(departement.code))

        if region is not None:
            query = query.join(Commune).options(
                orm.contains_eager(models.Service.commune_)
            )
            query = query.filter(Commune.region == region.code)

        if code_commune is not None:
            query = query.filter(models.Service.code_insee == code_commune)

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
            include_outdated=include_outdated,
        )

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
        types: list[TypologieService] | None = None,
        search_point: str | None = None,
        score_qualite_minimum: float | None = None,
        include_outdated: bool | None = False,
        deduplicate: bool | None = False,
    ):
        query = (
            sqla.select(models.Service)
            .join(models.Structure)
            .options(orm.contains_eager(models.Service.structure))
        )
        query = self.filter_restricted(query, request)

        if commune_instance is not None:
            # filter by zone de diffusion
            query = query.filter(
                sqla.or_(
                    models.Service.zone_diffusion_type.is_(None),
                    models.Service.zone_diffusion_type
                    == self.schema.ZoneDiffusionType.PAYS.value,
                    sqla.and_(
                        models.Service.zone_diffusion_type
                        == self.schema.ZoneDiffusionType.COMMUNE.value,
                        models.Service.zone_diffusion_code == commune_instance.code,
                    ),
                    sqla.and_(
                        models.Service.zone_diffusion_type
                        == self.schema.ZoneDiffusionType.EPCI.value,
                        sqla.literal(commune_instance.siren_epci).contains(
                            models.Service.zone_diffusion_code
                        ),
                    ),
                    sqla.and_(
                        models.Service.zone_diffusion_type
                        == self.schema.ZoneDiffusionType.DEPARTEMENT.value,
                        models.Service.zone_diffusion_code
                        == commune_instance.departement,
                    ),
                    sqla.and_(
                        models.Service.zone_diffusion_type
                        == self.schema.ZoneDiffusionType.REGION.value,
                        models.Service.zone_diffusion_code == commune_instance.region,
                    ),
                )
            )

            src_geometry = sqla.cast(
                geoalchemy2.functions.ST_MakePoint(
                    models.Service.longitude, models.Service.latitude
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
                    models.Service.modes_accueil.contains(
                        sqla.literal([self.schema.ModeAccueil.A_DISTANCE.value])
                    ),
                )
            )

            # annotate distance
            query = query.add_columns(
                (
                    sqla.case(
                        (
                            models.Service.modes_accueil.contains(
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
            include_outdated=include_outdated,
        )

        if deduplicate:
            query = query.filter(
                sqla.or_(
                    models.Structure.cluster_best_duplicate.is_(None),
                    models.Service._di_structure_surrogate_id
                    == models.Structure.cluster_best_duplicate,
                )
            )

        query = query.order_by(sqla.column("distance").nulls_last())

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
    ) -> models.Service:
        service_instance = db_session.scalars(
            sqla.select(models.Service)
            .options(orm.selectinload(models.Service.structure))
            .filter_by(source=source)
            .filter_by(id=id_)
        ).first()

        if service_instance is None:
            raise fastapi.HTTPException(status_code=404)

        return service_instance


class ServiceLayerV0(
    ServiceLayer[
        SchemaV0.Thematique,
        SchemaV0.Frais,
        SchemaV0.Profil,
        SchemaV0.TypologieService,
        SchemaV0.TypologieStructure,
        SchemaV0.LabelNational,
        SchemaV0.ModeAccueil,
        SchemaV0.CodeCommune,
    ]
):
    schema = SchemaV0()


class ServiceLayerV1(
    ServiceLayer[
        SchemaV1.Thematique,
        SchemaV1.Frais,
        SchemaV1.Profil,
        SchemaV1.TypologieService,
        SchemaV1.TypologieStructure,
        SchemaV1.LabelNational,
        SchemaV1.ModeAccueil,
        SchemaV1.CodeCommune,
    ]
):
    schema = SchemaV1()

    def list_structures(
        self,
        request: fastapi.Request,
        db_session: orm.Session,
        sources: list[str] | None = None,
        typologie: SchemaV1.TypologieStructure | None = None,
        label_national: SchemaV1.LabelNational | None = None,
        departement: Departement | None = None,
        region: Region | None = None,
        commune_code: SchemaV1.CodeCommune | None = None,
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
