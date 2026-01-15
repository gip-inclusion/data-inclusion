{# !!! THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY. !!! #}

{% macro udf__reseau_alpha_opening_hours() %}

DROP FUNCTION IF EXISTS processings.reseau_alpha_opening_hours;

CREATE OR REPLACE FUNCTION processings.reseau_alpha_opening_hours(data JSONB)
RETURNS TEXT
AS $$

"""Conversion des horaires reseau alpha vers le format OpenStreetMap.

Les models pydantic suivant décrivent la structure des données d'horaires
fournies par reseau alpha et permettent de les convertir au format
OpenStreetMap.
"""

import enum
import itertools
import operator
from datetime import datetime, time
from typing import Annotated

import pydantic
import pydantic.alias_generators


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        alias_generator=pydantic.alias_generators.to_camel,
    )


class Jour(enum.Enum):
    LUNDI = "lundi"
    MARDI = "mardi"
    MERCREDI = "mercredi"
    JEUDI = "jeudi"
    VENDREDI = "vendredi"
    SAMEDI = "samedi"
    DIMANCHE = "dimanche"

    def to_osm_weekday(self) -> str:
        return {
            Jour.LUNDI: "Mo",
            Jour.MARDI: "Tu",
            Jour.MERCREDI: "We",
            Jour.JEUDI: "Th",
            Jour.VENDREDI: "Fr",
            Jour.SAMEDI: "Sa",
            Jour.DIMANCHE: "Su",
        }[self]


def parse_time(value: str | None) -> time | None:
    return datetime.fromisoformat(value).time() if value is not None else None


class HorairesItem(BaseModel):
    jour: Jour
    debut: Annotated[
        time | None,
        pydantic.Field(alias="dateDebut"),
        pydantic.BeforeValidator(parse_time),
    ]
    fin: Annotated[
        time | None,
        pydantic.Field(alias="dateFin"),
        pydantic.BeforeValidator(parse_time),
    ]

    def weekday(self) -> str:
        return self.jour.to_osm_weekday()

    def time_selector(self) -> str | None:
        if self.debut is None or self.fin is None:
            return None
        return f"{self.debut.strftime('%H:%M')}-{self.fin.strftime('%H:%M')}"


def group_by_consecutive_weekdays(
    items: list[HorairesItem],
) -> list[list[HorairesItem]]:
    return [
        [item for _, item in group]
        for _, group in itertools.groupby(
            enumerate(items),
            key=lambda t: t[0] - list(Jour).index(t[1].jour),
        )
    ]


class Horaires(pydantic.RootModel[list[HorairesItem]]):
    def groups(self) -> list[list[HorairesItem]]:
        """Group by identical opening hours."""
        return [
            list(group)
            for _, group in itertools.groupby(
                self.root,
                key=operator.attrgetter("debut", "fin"),
            )
        ]

    def to_osm(self) -> str | None:
        if len(self.root) == 0:
            return None

        osm_rules = []

        for group in self.groups():
            # all items in group share the same time selector
            time_selector = group[0].time_selector()

            weekday_selectors = []
            for consecutive_groups in group_by_consecutive_weekdays(group):
                # consecutive days are grouped
                if len(consecutive_groups) == 1:
                    weekday_selector = consecutive_groups[0].weekday()
                else:
                    weekday_selector = "-".join(
                        [
                            consecutive_groups[0].weekday(),
                            consecutive_groups[-1].weekday(),
                        ]
                    )
                weekday_selectors.append(weekday_selector)

            rule = (
                f"{','.join(weekday_selectors)} {time_selector} open"
                if time_selector is not None
                else f"{','.join(weekday_selectors)} open"
            )
            osm_rules.append(rule)

        return "; ".join(osm_rules)


def to_osm(data: str | list) -> str | None:
    if isinstance(data, str):
        return Horaires.model_validate_json(data).to_osm()
    else:
        return Horaires.model_validate(data).to_osm()


return to_osm(data)

$$ LANGUAGE plpython3u;

{% endmacro %}
