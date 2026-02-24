{% macro udf__monenfant_opening_hours() %}

DROP FUNCTION IF EXISTS processings.monenfant_opening_hours;

CREATE OR REPLACE FUNCTION processings.monenfant_opening_hours(data JSONB)
RETURNS TEXT
AS $$
    """Conversion des horaires monenfant vers le format OpenStreetMap.

    Les models pydantic suivant décrivent la structure des données d'horaires
    fournies par monenfant.fr et permettent de les convertir au format
    OpenStreetMap.
    """

    from typing import Annotated, Literal
    import enum
    import functools

    import pydantic
    import pydantic.alias_generators


    class BaseModel(pydantic.BaseModel):
        model_config = pydantic.ConfigDict(
            alias_generator=pydantic.alias_generators.to_camel,
        )


    class CreneauHoraire(BaseModel):
        start: str
        end: str

        def to_osm(self) -> str:
            return f"{self.start.zfill(2)}-{self.end.zfill(2)}"


    class JoursHoraires(BaseModel):
        lundi: list[CreneauHoraire] | None
        mardi: list[CreneauHoraire] | None
        mercredi: list[CreneauHoraire] | None
        jeudi: list[CreneauHoraire] | None
        vendredi: list[CreneauHoraire] | None
        samedi: list[CreneauHoraire] | None
        dimanche: list[CreneauHoraire] | None

        monday_to_friday_week: bool | None = False

        def to_osm(self) -> str:
            def handle_creneau_horaire_list(creneau_list: list[CreneauHoraire]) -> str:
                return ",".join([c.to_osm() for c in creneau_list])

            if self.monday_to_friday_week:
                weekday_selector = "Mo-Fr"

                if self.lundi is not None and len(self.lundi) > 0:
                    time_selector = handle_creneau_horaire_list(self.lundi)
                    return f"{weekday_selector} {time_selector} open"
                else:
                    return f"{weekday_selector} open"

            return ", ".join(
                f"{weekday} {handle_creneau_horaire_list(horaires)} open"
                if horaires is not None and len(horaires) > 0
                else f"{weekday} open"
                for weekday, horaires in [
                    ("Mo", self.lundi),
                    ("Tu", self.mardi),
                    ("We", self.mercredi),
                    ("Th", self.jeudi),
                    ("Fr", self.vendredi),
                    ("Sa", self.samedi),
                    ("Su", self.dimanche),
                ]
            )


    class PeriodeFermeture(enum.StrEnum):
        JAMAIS = "jamais"
        JUILLET = "juillet"
        AOUT = "aout"
        NOEL_NOUVEL_AN = "noel_nouvel_an"

        def to_osm(self) -> str:
            selector = {
                PeriodeFermeture.JUILLET: "Jul",
                PeriodeFermeture.AOUT: "Aug",
                PeriodeFermeture.NOEL_NOUVEL_AN: "Dec 25-Jan 1",
            }.get(self, None)

            if selector is None:
                return ""

            return f"; {selector} closed"


    class Calendrier(BaseModel):
        rendez_vous: bool
        jours_horaires: JoursHoraires
        periodes_fermetures: list[PeriodeFermeture]

        def to_osm(self) -> str:
            return "".join(
                [
                    self.jours_horaires.to_osm(),
                    "".join([p.to_osm() for p in self.periodes_fermetures])
                    if PeriodeFermeture.JAMAIS not in self.periodes_fermetures
                    else "",
                ]
            )

    @functools.lru_cache(maxsize=1024)
    def to_osm(data: str) -> str:
        return Calendrier.model_validate_json(data).to_osm()

    return to_osm(data)

$$ LANGUAGE plpython3u;

{% endmacro %}
