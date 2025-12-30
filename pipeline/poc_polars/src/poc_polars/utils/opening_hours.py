import enum
import functools
import itertools
import operator
from typing import Annotated, Literal

import pydantic
import pydantic.alias_generators


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        alias_generator=lambda field_name: pydantic.alias_generators.to_camel(
            field_name
        ).replace("Rdv", "RDV"),
    )


class Jour(enum.Enum):
    LUNDI = 1
    MARDI = 2
    MERCREDI = 3
    JEUDI = 4
    VENDREDI = 5

    def to_osm_weekday(self) -> str:
        return {
            Jour.LUNDI: "Mo",
            Jour.MARDI: "Tu",
            Jour.MERCREDI: "We",
            Jour.JEUDI: "Th",
            Jour.VENDREDI: "Fr",
        }[self]


def parse_bool(value: Literal["O", "N"]) -> bool | None:
    return {"O": True, "N": False}.get(value, None)


def format_timespan(start: str, end: str) -> str:
    return f"{start.zfill(4)}-{end.zfill(4)}"


class HorairesItem(BaseModel):
    jour: Jour
    horaire_ferme: Annotated[bool, pydantic.BeforeValidator(parse_bool)]
    horaire_en_continu: Annotated[bool, pydantic.BeforeValidator(parse_bool)]
    ouverture_matin: str | None = None
    ouverture_apres_midi: str | None = None
    fermeture_matin: str | None = None
    fermeture_apres_midi: str | None = None
    horaire_ferme_rdv: Annotated[bool, pydantic.BeforeValidator(parse_bool)] | None = (
        None
    )
    horaire_en_continu_rdv: (
        Annotated[bool, pydantic.BeforeValidator(parse_bool)] | None
    ) = None
    ouverture_matin_rdv: str | None = None
    ouverture_apres_midi_rdv: str | None = None
    fermeture_matin_rdv: str | None = None
    fermeture_apres_midi_rdv: str | None = None

    def weekday(self) -> str:
        return self.jour.to_osm_weekday()

    def time_selector(self) -> str | None:
        if self.horaire_ferme:
            return None

        timespans = []
        if (
            self.horaire_en_continu
            and self.ouverture_matin is not None
            and self.fermeture_apres_midi is not None
        ):
            timespans.append(
                format_timespan(self.ouverture_matin, self.fermeture_apres_midi)
            )
        else:
            if self.ouverture_matin is not None and self.fermeture_matin is not None:
                timespans.append(
                    format_timespan(self.ouverture_matin, self.fermeture_matin)
                )
            if (
                self.ouverture_apres_midi is not None
                and self.fermeture_apres_midi is not None
            ):
                timespans.append(
                    format_timespan(
                        self.ouverture_apres_midi, self.fermeture_apres_midi
                    )
                )

        return ",".join(timespans) if timespans else None


class Horaires(pydantic.RootModel[list[HorairesItem]]):
    def groups(self) -> list[list[HorairesItem]]:
        return [
            list(group)
            for _, group in itertools.groupby(
                self.root,
                key=operator.attrgetter(
                    "horaire_ferme",
                    "horaire_en_continu",
                    "ouverture_matin",
                    "fermeture_matin",
                    "ouverture_apres_midi",
                    "fermeture_apres_midi",
                ),
            )
        ]

    def to_osm(self) -> str:
        osm_rules = [
            f'{group[0].weekday()} {time_selector} open "Sans rendez-vous"'
            if len(group) == 1
            else (
                f"{group[0].weekday()}-{group[-1].weekday()} {time_selector}"
                ' open "Sans rendez-vous"'
            )
            for group in self.groups()
            if (time_selector := group[0].time_selector()) is not None
        ]
        osm_rules.append("PH off")
        return "; ".join(osm_rules)


@functools.lru_cache(maxsize=1024)
def _cached_to_osm(data: str) -> str:
    return Horaires.model_validate_json(data).to_osm()


def france_travail_opening_hours(data: str | list | dict | None) -> str | None:
    if data is None:
        return None
    try:
        if isinstance(data, str):
            return _cached_to_osm(data)
        if isinstance(data, list | dict):
            return Horaires.model_validate(data).to_osm()
        return None
    except Exception:
        return None
