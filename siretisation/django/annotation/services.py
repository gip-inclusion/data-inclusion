import functools
import re
from typing import Optional

import tqdm
from cnfs.models import Permanence

from django.contrib.gis import geos, measure
from django.contrib.gis.db.models.functions import GeometryDistance
from django.contrib.postgres import search
from django.db import models, transaction

from annotation.models import Dataset, DatasetRow
from sirene.models import Establishment


def import_dataset(
    dataset_rows_list: list[dict],
    dataset_label: str,
    organization_id: str,
):
    with transaction.atomic():
        dataset_instance, _ = Dataset.objects.get_or_create(
            label=dataset_label,
            organization_id=organization_id,
        )

        for dataset_row_dict in tqdm.tqdm(dataset_rows_list):
            row_instance = DatasetRow.objects.create(dataset=dataset_instance, data=dataset_row_dict)

            if "adresse" in row_instance.data:
                similar_address = search_similar_address(
                    address=row_instance.data["adresse"],
                    postal_code=row_instance.data["code_postal"],
                )
                if similar_address is not None:
                    row_instance.similar_address = similar_address
                    row_instance.save()


def search_similar_address(
    address: Optional[str],
    postal_code: Optional[str],
) -> Optional[str]:
    if postal_code is None or address is None:
        return None

    establishment_qs = Establishment.objects.filter(postal_code__startswith=postal_code[:2])

    establishment_qs = establishment_qs.annotate(
        address_similarity=search.TrigramWordSimilarity(address, "address1")
    ).order_by("-address_similarity")

    establishment_data = establishment_qs.first()

    if establishment_data is None:
        return None

    if establishment_data.address_similarity < 0.4:
        return None

    return re.sub(r"^\d* ", "", establishment_data.address1)


def search_sirene(
    *,
    adresse: Optional[str] = None,
    name: Optional[str] = None,
    postal_code: Optional[str] = None,
    siret: Optional[str] = None,
    naf_activities: Optional[list[tuple[str, str]]] = None,
) -> models.QuerySet:
    if siret is not None:
        siret = siret.replace(" ", "")
        if len(siret) == 14:
            return Establishment.objects.filter(siret=siret)

    # prevent searches based on the name only, because trigram similarity can not be
    # used on the whole sirene database
    if not postal_code:
        return Establishment.objects.none()

    establishment_qs = Establishment.objects

    if postal_code is not None:
        establishment_qs = establishment_qs.filter(postal_code__startswith=postal_code)

    if naf_activities is not None and naf_activities != []:
        lookups = [models.Q(**{f"ape__{level}_code": code}) for level, code in naf_activities]
        lookup = functools.reduce(lambda acc, v: acc | v, lookups)
        establishment_qs = establishment_qs.filter(lookup)

    if siret is not None:
        establishment_qs = establishment_qs.filter(siret__contains=siret)

    if name:
        name = re.sub(r"\W", " ", name)
        establishment_qs = establishment_qs.annotate(
            name_similarity=search.TrigramWordSimilarity(name, "name")
        ).order_by("-name_similarity")

        if adresse is not None:
            establishment_qs = establishment_qs.filter(address1__unaccent__iregex=adresse)

    elif adresse is not None:
        adresse = re.sub(r"\W", " ", adresse)
        establishment_qs = establishment_qs.annotate(
            adresse_similarity=search.TrigramWordSimilarity(adresse, "address1")
        ).order_by("-adresse_similarity")

    return establishment_qs.all()


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
