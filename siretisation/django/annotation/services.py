from typing import Optional

from django.conf import settings
from django.contrib.gis import geos, measure
from django.contrib.gis.db.models.functions import GeometryDistance
from django.db import models

from cnfs.models import Permanence
from common import annuaire_entreprises, cog


def search_sirene(
    *,
    address: Optional[str] = None,
    name: Optional[str] = None,
    code_insee: Optional[str] = None,
    siret: Optional[str] = None,
    naf_section: Optional[list[str]] = None,
    naf_ape: Optional[list[str]] = None,
    is_association: Optional[bool] = None,
) -> list:
    annuaire_entreprises_client = annuaire_entreprises.AnnuaireEntreprisesClient(
        base_url=settings.ANNUAIRE_ENTREPRISES_API_URL
    )

    params = {
        "section_activite_principale": naf_section,
        "activite_principale": naf_ape,
        "est_entrepreneur_individuel": False,
    }

    # if siret given, focus on it and exclude other search terms
    if siret is not None and len(siret) == 14:
        params["q"] = siret
    else:
        params["q"] = " ".join([d for d in [name, address] if d is not None])

    if code_insee is not None:
        if len(code_insee) == 5:
            params["code_commune"] = code_insee
        elif code_insee in cog.code_departements:
            params["departement"] = code_insee

    if is_association is not None:
        params["est_association"] = is_association

    return annuaire_entreprises_client.search(**params)["results"]


def search_cnfs(
    *,
    siret: Optional[str] = None,
    longitude: Optional[float] = None,
    latitude: Optional[float] = None,
) -> models.QuerySet:
    if siret is not None:
        siret = siret.replace(" ", "")
        if len(siret) == 14:
            return Permanence.objects.filter(siret=siret)

    permanence_qs = Permanence.objects

    if longitude is None or latitude is None:
        return permanence_qs.none()

    point = geos.GEOSGeometry(f"SRID=4326;POINT({longitude} {latitude})")
    permanence_qs = permanence_qs.exclude(siret__isnull=True)
    permanence_qs = permanence_qs.filter(position__distance_lte=(point, measure.D(m=100)))
    permanence_qs = permanence_qs.annotate(distance=GeometryDistance("position", point))
    permanence_qs = permanence_qs.order_by("distance")

    return permanence_qs.all()
