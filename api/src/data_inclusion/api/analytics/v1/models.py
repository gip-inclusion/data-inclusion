from sqlalchemy.orm import Mapped

from data_inclusion.api.core import db


class ConsultStructureEvent(db.Base):
    __tablename__ = "api__consult_structure_events_v1"

    id: Mapped[db.uuid_pk]
    created_at: Mapped[db.timestamp]
    user: Mapped[str]
    structure_id: Mapped[str]


class ConsultServiceEvent(db.Base):
    __tablename__ = "api__consult_service_events_v1"

    id: Mapped[db.uuid_pk]
    created_at: Mapped[db.timestamp]
    user: Mapped[str]
    service_id: Mapped[str]
    score_qualite: Mapped[float | None]


class ListServicesEvent(db.Base):
    __tablename__ = "api__list_services_events_v1"

    id: Mapped[db.uuid_pk]
    created_at: Mapped[db.timestamp]
    user: Mapped[str]
    sources: Mapped[list[str] | None]
    thematiques: Mapped[list[str] | None]
    code_departement: Mapped[str | None]
    code_region: Mapped[str | None]
    code_commune: Mapped[str | None]
    frais: Mapped[list[str] | None]
    publics: Mapped[list[str] | None]
    modes_accueil: Mapped[list[str] | None]
    types: Mapped[list[str] | None]
    recherche_public: Mapped[str | None]
    score_qualite_minimum: Mapped[float | None]


class ListStructuresEvent(db.Base):
    __tablename__ = "api__list_structures_events_v1"

    id: Mapped[db.uuid_pk]
    created_at: Mapped[db.timestamp]
    user: Mapped[str]
    sources: Mapped[list[str] | None]
    reseaux_porteurs: Mapped[str | None]
    code_departement: Mapped[str | None]
    code_region: Mapped[str | None]
    code_commune: Mapped[str | None]
    exclure_doublons: Mapped[bool | None]


class SearchServicesEvent(db.Base):
    __tablename__ = "api__search_services_events_v1"

    id: Mapped[db.uuid_pk]
    created_at: Mapped[db.timestamp]
    user: Mapped[str]
    first_services: Mapped[list[dict] | None]
    total_services: Mapped[int | None]
    sources: Mapped[list[str] | None]
    code_commune: Mapped[str | None]
    lat: Mapped[float | None]
    lon: Mapped[float | None]
    thematiques: Mapped[list[str] | None]
    frais: Mapped[list[str] | None]
    modes_accueil: Mapped[list[str] | None]
    publics: Mapped[list[str] | None]
    types: Mapped[list[str] | None]
    recherche_public: Mapped[str | None]
    score_qualite_minimum: Mapped[float | None]
    exclure_doublons: Mapped[bool | None]
