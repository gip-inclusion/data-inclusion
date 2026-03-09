from sqlalchemy.orm import Mapped, mapped_column

from data_inclusion.api.core.db import Base


class FrameworkTable:
    value: Mapped[str] = mapped_column(primary_key=True)
    label: Mapped[str]
    description: Mapped[str | None]

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(value={self.value})>"


class Frais(FrameworkTable, Base):
    __tablename__ = "api__frais_v1"


class ModeAccueil(FrameworkTable, Base):
    __tablename__ = "api__modes_accueil_v1"


class ModeMobilisation(FrameworkTable, Base):
    __tablename__ = "api__modes_mobilisation_v1"


class PersonneMobilisatrice(FrameworkTable, Base):
    __tablename__ = "api__personnes_mobilisatrices_v1"


class Public(FrameworkTable, Base):
    __tablename__ = "api__publics_v1"


class ReseauPorteur(FrameworkTable, Base):
    __tablename__ = "api__reseaux_porteurs_v1"


class Thematique(FrameworkTable, Base):
    __tablename__ = "api__thematiques_v1"


class TypeService(FrameworkTable, Base):
    __tablename__ = "api__types_services_v1"
